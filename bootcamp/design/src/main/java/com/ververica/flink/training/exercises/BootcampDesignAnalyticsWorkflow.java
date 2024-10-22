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
 * Workflow that does simple analytics, where we calculate
 * per-customer/per-hour transactions that aren't completed.
 *
 * We also save abandoned cart items in a new sink.
 *
 */
public class BootcampDesignAnalyticsWorkflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<AbandonedCartItem> abandonedSink;
    protected Sink<KeyedWindowResult> analyticsSink;

    public BootcampDesignAnalyticsWorkflow() {
    }

    public BootcampDesignAnalyticsWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampDesignAnalyticsWorkflow setAbandonedSink(Sink<AbandonedCartItem> abandonedSink) {
        this.abandonedSink = abandonedSink;
        return this;
    }

    public BootcampDesignAnalyticsWorkflow setAnalyticsSink(Sink<KeyedWindowResult> analyticsSink) {
        this.analyticsSink = analyticsSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(analyticsSink, "analyticsSink must be set");
        Preconditions.checkNotNull(abandonedSink, "abandonedSink must be set");

        // Assign timestamps & watermarks. Note that we don't filter out pending
        // transactions, as those are the ones we care about.
        DataStream<ShoppingCartRecord> watermarked = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        // TODO - create a DataStream<AbandonedCartItem> stream from watermarked.
        DataStream<AbandonedCartItem> abandoned = null;

        // TODO - use the abandoned stream to generate a stream of the analytics result, and
        // send to the analyticsSink.

        // TODO - save the abandoned stream to the abandonedSink.
        abandoned
                .sinkTo(abandonedSink);
    }


}