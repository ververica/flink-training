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
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.AbandonedCartItem;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import com.ververica.flink.training.solutions.BootcampDesignAnalyticsSolutionWorkflow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Given a stream of AbandonedCartItem records, identify customers who seem to be
 * engaged in product-boosting behavior, by adding a lot of products to carts
 * that are never completed (purchased).
 */
public class BootcampDesignDetectionWorkflow {

    private static final long DEFAULT_ABANDONED_LIMIT = 1;

    protected DataStream<AbandonedCartItem> abandonedStream;
    protected Sink<KeyedWindowResult> badCustomerSink;

    protected long maxAbandonedPerMinute = DEFAULT_ABANDONED_LIMIT;

    public BootcampDesignDetectionWorkflow() {
    }

    public BootcampDesignDetectionWorkflow setAbandonedStream(DataStream<AbandonedCartItem> abandonedStream) {
        this.abandonedStream = abandonedStream;
        return this;
    }

    public BootcampDesignDetectionWorkflow setBadCustomerSink(Sink<KeyedWindowResult> badCustomerSink) {
        this.badCustomerSink = badCustomerSink;
        return this;
    }

    public BootcampDesignDetectionWorkflow setMaxAbandonedPerMinute(long maxAbandonedPerMinute) {
        this.maxAbandonedPerMinute = maxAbandonedPerMinute;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(abandonedStream, "abandonedStream must be set");
        Preconditions.checkNotNull(badCustomerSink, "badCustomerSink must be set");

        // Assign timestamps & watermarks. Note that we don't filter out pending
        // transactions, as those are the ones we care about.
        DataStream<AbandonedCartItem> watermarked = abandonedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AbandonedCartItem>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        // Key by customer, tumbling window per hour.
        // Count number of transactions, then filter to only the suspected items.
        final long abandonedLimit = maxAbandonedPerMinute;
        watermarked
                .keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountTransactionsAggregator(), new SetKeyAndTimeFunction())
                .filter(r -> r.getResult() >= abandonedLimit)
                .sinkTo(badCustomerSink);
    }

    private static class CountTransactionsAggregator implements AggregateFunction<AbandonedCartItem, Set<String>, Long> {
        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(AbandonedCartItem value, Set<String> acc) {
            acc.add(value.getTransactionId());
            return acc;
        }

        @Override
        public Long getResult(Set<String> acc) {
            return (long)acc.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }

}