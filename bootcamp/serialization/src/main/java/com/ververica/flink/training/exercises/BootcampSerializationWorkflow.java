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

package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.WindowAllResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Improve the performance of the workflow (throughput) by making
 * changes recommended in the lab's README
 */
public class BootcampSerializationWorkflow {

    // Maximum time between transactions where they will still be considered a
    // single session.
    protected static final Duration MAX_SESSION_GAP = Duration.ofMinutes(1);

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<KeyedWindowResult> oneMinuteSink;
    protected Sink<WindowAllResult> fiveMinuteSink;
    protected Sink<KeyedWindowResult> longestTransactionsSink;
    protected int transactionWindowInMinutes = 5;

    public BootcampSerializationWorkflow() {
    }

    public BootcampSerializationWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampSerializationWorkflow setOneMinuteSink(Sink<KeyedWindowResult> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public BootcampSerializationWorkflow setFiveMinuteSink(Sink<WindowAllResult> fiveMinuteSink) {
        this.fiveMinuteSink = fiveMinuteSink;
        return this;
    }

    public BootcampSerializationWorkflow setLongestTransactionsSink(Sink<KeyedWindowResult> longestTransactionsSink) {
        this.longestTransactionsSink = longestTransactionsSink;
        return this;
    }

    public BootcampSerializationWorkflow setTransactionsWindowInMinutes(int transactionWindowInMinutes) {
        this.transactionWindowInMinutes = transactionWindowInMinutes;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(oneMinuteSink, "oneMinuteSink must be set");
        Preconditions.checkNotNull(fiveMinuteSink, "fiveMinuteSink must be set");
        Preconditions.checkNotNull(longestTransactionsSink, "longestTransactionsSink must be set");

        // Assign timestamps & watermarks, and
        DataStream<ShoppingCartRecord> watermarkedStream = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        // Filter out pending carts, key by country, tumbling window per minute
        DataStream<KeyedWindowResult> oneMinuteStream = watermarkedStream
                .filter(r -> r.isTransactionCompleted())
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
                .window(EventTimeSessionWindows.withGap(MAX_SESSION_GAP))
                .aggregate(new FindTransactionBoundsFunction(), new SetDurationAndTimeFunction())

                // Window by configurable window size, aggregate using PriorityQueue
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(transactionWindowInMinutes)))
                .aggregate(new FindLongestTransactions(), new EmitLongestTransactions())
                .sinkTo(longestTransactionsSink);
    }

    // ========================================================================================
    // Classes for doing aggregation to calculate per-1 minute item counts.
    // ========================================================================================

    private static class CountCartItemsAggregator implements AggregateFunction<ShoppingCartRecord, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ShoppingCartRecord value, Long acc) {
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
    // Classes for doing aggregation to calculate shopping cart transaction durations
    // ========================================================================================

    /*
     * Find the earliest start and final end time for each transaction
     */
    private static class FindTransactionBoundsFunction implements AggregateFunction<ShoppingCartRecord, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(-1L, -1L);
        }

        @Override
        public Tuple2<Long, Long> add(ShoppingCartRecord value, Tuple2<Long, Long> acc) {
            long transactionTime = value.getTransactionTime();
            if (value.isTransactionCompleted()) {
                acc.f1 = transactionTime;
            } else if ((acc.f0 == -1) || (transactionTime < acc.f0)) {
                acc.f0 = transactionTime;
            }

            return acc;
        }

        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
            return acc;
        }

        /**
         * Merge the two Tuples<start time, end time> by setting the resulting
         * start time to the min of the two, and the end time to the max of
         * the two. If the time is -1, ignore it since it hasn't be set yet.
         *
         * @param a An accumulator to merge
         * @param b Another accumulator to merge
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            if (a.f0 == -1) {
                a.f0 = b.f0;
            } else if ((b.f0 != -1) && (b.f0 < a.f0)) {
                a.f0 = b.f0;
            }

            if (a.f1 == -1) {
                a.f1 = b.f1;
            } else if ((b.f1 != -1) && (b.f1 > a.f1)) {
                a.f1 = b.f1;
            }

            return a;
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

    private static class FindLongestTransactions implements AggregateFunction<KeyedWindowResult,
            PriorityQueue<KeyedWindowResult>, List<KeyedWindowResult>> {
        @Override
        public PriorityQueue<KeyedWindowResult> createAccumulator() {
            return new PriorityQueue<>(new TransactionDurationComparator());
        }

        @Override
        public PriorityQueue<KeyedWindowResult> add(KeyedWindowResult value, PriorityQueue<KeyedWindowResult> acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public List<KeyedWindowResult> getResult(PriorityQueue<KeyedWindowResult> acc) {
            List<KeyedWindowResult> result = new ArrayList<>();
            int numToReturn = Math.min(acc.size(), 2);
            for (int i = 0; i < numToReturn; i++) {
                result.add(acc.remove());
            }

            return result;
        }

        @Override
        public PriorityQueue<KeyedWindowResult> merge(PriorityQueue<KeyedWindowResult> a, PriorityQueue<KeyedWindowResult> b) {
            a.addAll(b);
            return a;
        }
    }

    private static class TransactionDurationComparator
            implements Comparator<KeyedWindowResult> {
        @Override
        public int compare(KeyedWindowResult o1, KeyedWindowResult o2) {
            // Return inverse sort order, longest first
            return Long.compare(o2.getResult(), o1.getResult());
        }
    }

    private static class EmitLongestTransactions extends ProcessAllWindowFunction<List<KeyedWindowResult>,
            KeyedWindowResult, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<List<KeyedWindowResult>> elements,
                            Collector<KeyedWindowResult> out) throws Exception {
            for (KeyedWindowResult e : elements.iterator().next()) {
                out.collect(e);
            }
        }
    }
}