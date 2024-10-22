/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.WindowAllResult;
import com.ververica.flink.training.exercises.BootcampSerializationWorkflow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Solution to the exercises in the eCommerce serialization lab, in increasing
 * order of complexity...
 *
 * 1. We strip down the incoming records and convert to a TrimmedShoppingCartRecord
 * 2. TrimmedShoppingCartRecord is serializable as a POJO
 * 3. Use KeyedProcessFunction to find duration, versus session window
 * 4. Use simple structure for top two aggregation, versus priority queue
 *
 */
public class BootcampSerializationSolutionWorkflow extends BootcampSerializationWorkflow {
    private static final Logger LOGGER = LoggerFactory.getLogger(BootcampSerializationSolutionWorkflow.class);

    private static final long MAX_SESSION_GAP_MS = MAX_SESSION_GAP.toMillis();

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(oneMinuteSink, "oneMinuteSink must be set");
        Preconditions.checkNotNull(fiveMinuteSink, "fiveMinuteSink must be set");
        Preconditions.checkNotNull(longestTransactionsSink, "longestTransactionsSink must be set");

        // Assign timestamps & watermarks
        DataStream<TrimmedShoppingCart> watermarkedStream = cartStream
                // Convert to a smaller/better version of ShoppingCartRecord.
                .map(r -> new TrimmedShoppingCart(r))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrimmedShoppingCart>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        DataStream<KeyedWindowResult> oneMinuteStream = watermarkedStream
                // Filter out pending carts
                .filter(r -> r.isTransactionCompleted())
                // Key by country, tumbling window per minute
                .keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

        oneMinuteStream
                .sinkTo(oneMinuteSink);

        // Calculate the 5-minute window counts, using the 1-minute window count result as output. We know
        // that the event time for records in the oneMinuteStream will be set by Flink to be the end of
        // the window, so we can do a windowAll (which means a single task, no parallelism) to do a second
        // aggregation on the more-granular 1-minute results.
        oneMinuteStream
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(new OneMinuteWindowCountAggregator(), new SetTimeFunction())
                .sinkTo(fiveMinuteSink);

        // Find the top 2 transactions (by duration) per configurable window size.
        watermarkedStream
                // Key by transaction id, window by transaction (session) and calculate duration.
                // Generate result as KeyedWindowResult(transaction id, time, duration)
                .keyBy(r -> r.getTransactionId())
                .process(new FindTransactionBoundsFunction())

                // Use a global window, configurable duration, and aggregate with a simple record
                // that tracks the two longest transactions.
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(transactionWindowInMinutes)))
                .aggregate(new FindLongestTransactions(), new EmitLongestTransactions())
                .sinkTo(longestTransactionsSink);
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-1 minute item counts.
    // ========================================================================================

    private static class CountCartItemsAggregator implements AggregateFunction<TrimmedShoppingCart, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(TrimmedShoppingCart value, Long acc) {
            for (CartItem item : value.getItems()) {
                acc += item.getQuantity();
            }

            return acc;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class SetTimeFunction extends ProcessAllWindowFunction<Long, WindowAllResult, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<Long> elements, Collector<WindowAllResult> out) throws Exception {
            out.collect(new WindowAllResult(ctx.window().getStart(), elements.iterator().next()));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-5 minute item counts, using the output
    // of the 1-minute aggregation as input
    // ========================================================================================

    private static class OneMinuteWindowCountAggregator implements AggregateFunction<KeyedWindowResult, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(KeyedWindowResult value, Long acc) {
            return acc + value.getResult();
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }

    // ========================================================================================
    // Class for calculate shopping cart transaction durations
    // ========================================================================================

    private static class FindTransactionBoundsFunction extends KeyedProcessFunction<String, TrimmedShoppingCart, Tuple2<String, Long>> {

        // Earliest time of any uncompleted transaction
        private ValueState<Long> earliestTime;
        // Set if we have a timer running
        private ValueState<Long> timerTime;
        // Transaction time for the one completed transaction
        private ValueState<Long> endTime;

        @Override
        public void open(OpenContext openContext) throws Exception {
            earliestTime = getRuntimeContext().getState(new ValueStateDescriptor<>("earliestTime", Long.class));
            timerTime = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTime", Long.class));
            endTime = getRuntimeContext().getState(new ValueStateDescriptor<>("endTime", Long.class));
        }

        @Override
        public void processElement(TrimmedShoppingCart in, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            Long earliest = earliestTime.value();
            Long timer = timerTime.value();
            Long end = endTime.value();

            long transactionTime = in.getTransactionTime();

            // We assume properly ordered data, in that the completed transaction's time will always be >= any
            // uncompleted transactions, thus we don't need to do any special checks here.
            if (in.isTransactionCompleted()) {
                removeTimer(ctx);
                endTime.update(transactionTime);
                timerTime.update(transactionTime);
                startTimer(ctx);
            } else {
                if ((earliest == null) || (earliest > transactionTime)) {
                    earliestTime.update(transactionTime);
                }

                // See if we need to update the timerTime. If we have an end time
                // then we are good, otherwise if this is the first non-transaction
                // record, or it's later than our current timer, we want to stop the
                // potentially running timer, and start with the later time.
                if ((end == null) && ((earliest == null) || (transactionTime > timer))) {
                    removeTimer(ctx);
                    timerTime.update(transactionTime);
                    startTimer(ctx);
                }
            }
        }

        private void startTimer(Context ctx) throws IOException {
            ctx.timerService().registerEventTimeTimer(timerTime.value() + MAX_SESSION_GAP_MS);
        }

        private void removeTimer(Context ctx) throws IOException {
            if (timerTime.value() != null) {
                ctx.timerService().deleteEventTimeTimer(timerTime.value() + MAX_SESSION_GAP_MS);
                timerTime.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // Timer fired. If we have an end transaction, then we have a duration. If there's no start, assume it's
            // a single action. If we don't have an end transaction, assume it's an abandoned cart (do nothing).
            Long start = earliestTime.value();
            Long end = endTime.value();
            if (end != null) {
                if (start == null) {
                    start = end;
                }

                // If we just call out.collect(), the record's event time is set to the timer's timestamp. But
                // that's not what we want, as this timer is for when we end a session, so it's some time
                // after the end of the session. We need to do the funky cast of the collector to a TimestampedCollector,
                // which lets us set the timestamp to use when we call collect.
                TimestampedCollector<Tuple2<String, Long>> outWithTime = (TimestampedCollector)out;
                outWithTime.setAbsoluteTimestamp(end);
                outWithTime.collect(Tuple2.of(ctx.getCurrentKey(), end - start));
            }

            // Clear all our state
            earliestTime.clear();
            timerTime.clear();
            endTime.clear();
        }
    }

    private static class SetDurationAndTimeFunction extends ProcessWindowFunction<Tuple2<Long, Long>, KeyedWindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Tuple2<Long, Long>> elements, Collector<KeyedWindowResult> out) throws Exception {
            Tuple2<Long, Long> interval = elements.iterator().next();

            // If we didn't get a start (probably because the session window timed out, so all we got is
            // an end) or we never got an end (cart was abandoned) then just ignore this.
            if ((interval.f0 == -1) || (interval.f1 == -1)) {
                return;
            }

            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), interval.f1 - interval.f0));
        }
    }

    // ========================================================================================
    // Classes for doing aggregation to find the N longest transactions
    // ========================================================================================

    private static class FindLongestTransactions implements AggregateFunction<Tuple2<String, Long>,
            LongestTwoTransactions, List<Tuple2<String, Long>>> {
        @Override
        public LongestTwoTransactions createAccumulator() {
            return new LongestTwoTransactions();
        }

        @Override
        public LongestTwoTransactions add(Tuple2<String, Long> value, LongestTwoTransactions acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public List<Tuple2<String, Long>> getResult(LongestTwoTransactions acc) {
            List<Tuple2<String, Long>> result = new ArrayList<>();
            result.add(acc.getFirst());
            if (acc.getSecond() != null) {
                result.add(acc.getSecond());
            }

            return result;
        }

        @Override
        public LongestTwoTransactions merge(LongestTwoTransactions a, LongestTwoTransactions b) {
            if (b.getFirst() != null) {
                a.add(b.getFirst());

                if (b.getSecond() != null) {
                    a.add(b.getSecond());
                }
            }

            return a;
        }
    }

    public static class LongestTwoTransactions {
        private Tuple2<String, Long> first;
        private Tuple2<String, Long> second;

        public LongestTwoTransactions() {}

        public Tuple2<String, Long> getFirst() {
            return first;
        }

        public void setFirst(Tuple2<String, Long> first) {
            this.first = first;
        }

        public Tuple2<String, Long> getSecond() {
            return second;
        }

        public void setSecond(Tuple2<String, Long> second) {
            this.second = second;
        }

        public void add(Tuple2<String, Long> t) {
            if (first == null) {
                first = t;
            } else {
                if (t.f1 > first.f1) {
                    second = first;
                    first = t;
                } else if ((second == null) || (t.f1 > second.f1)) {
                    second = t;
                }
            }
        }
    }

    private static class EmitLongestTransactions extends ProcessAllWindowFunction<List<Tuple2<String, Long>>,
            KeyedWindowResult, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<List<Tuple2<String, Long>>> elements,
                            Collector<KeyedWindowResult> out) throws Exception {
            long windowStart = ctx.window().getStart();
            for (Tuple2<String, Long> e : elements.iterator().next()) {
                out.collect(new KeyedWindowResult(e.f0, windowStart, e.f1));
            }
        }
    }

}