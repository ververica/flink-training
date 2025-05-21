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

package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Workflow we run to generate results, which will fail once.
 *
 * We calculate a per-country/per-minute count of items in completed
 * shopping carts.
 */
@DoNotChangeThis
public class BootcampFailuresWorkflow {

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<String> resultSink;
    private boolean triggerFailure = true;

    public BootcampFailuresWorkflow() {
    }

    public BootcampFailuresWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampFailuresWorkflow setResultSink(Sink<String> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public BootcampFailuresWorkflow setTriggerFailure(boolean triggerFailure) {
        this.triggerFailure = triggerFailure;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks, and filter out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Key by country, tumbling window per minute
        filtered.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountItemsAggregator(),
                        triggerFailure ? new FailingSetKeyAndTimeFunction() : new SetKeyAndTimeFunction())
                .map(r -> r.toString())
                .sinkTo(resultSink);
    }

    private static class CountItemsAggregator implements AggregateFunction<ShoppingCartRecord, Long, Long> {
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

    /**
     * This is just like the ProcessWindowFunction we created for the windowing lab, except that one time
     * when we get a special result with 0 items, we'll throw an exception that causes the workflow to
     * fail.
     */
    private static class FailingSetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {

        private static final AtomicBoolean FAILURE_TRIGGERED = new AtomicBoolean(false);

        public void reset() {
            FAILURE_TRIGGERED.set(false);
        }

        public FailingSetKeyAndTimeFunction() {
            reset();
        }

        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            long itemCount = elements.iterator().next();
            // If it's our special result (item count of 0) and we haven't previously
            // thrown an exception, do that now.
            if ((itemCount == 0L) && !FAILURE_TRIGGERED.getAndSet(true)) {
                throw new IndexOutOfBoundsException("Some error in the workflow");
            }

            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), itemCount));
        }
    }

    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            long itemCount = elements.iterator().next();
            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), itemCount));
        }
    }


}