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

import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.WindowAllResult;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Add a global per-5 minute window as a second result. This should generate
 * WindowAllResult(time, count) results that are for all countries.
 */
public class BootcampWindowing2Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<KeyedWindowResult> oneMinuteSink;
    protected Sink<WindowAllResult> fiveMinuteSink;

    public BootcampWindowing2Workflow() {
    }

    public BootcampWindowing2Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampWindowing2Workflow setOneMinuteSink(Sink<KeyedWindowResult> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public BootcampWindowing2Workflow setFiveMinuteSink(Sink<WindowAllResult> fiveMinuteSink) {
        this.fiveMinuteSink = fiveMinuteSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(oneMinuteSink, "oneMinuteSink must be set");
        Preconditions.checkNotNull(fiveMinuteSink, "fiveMinuteSink must be set");

        // Assign timestamps & watermarks, and out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Key by country, tumbling window per minute
        DataStream<KeyedWindowResult> oneMinuteStream = filtered
                .keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

        oneMinuteStream
                .sinkTo(oneMinuteSink);

        // TODO - use a global window (unkeyed), generate results for a 5 minute window.
        // You can take advantage of the provided OneMinuteWindowCountAggregator and
        // SetTimeFunction classes.
        DataStream<WindowAllResult> fiveMinuteStream = null;

        fiveMinuteStream
                .sinkTo(fiveMinuteSink);

    }

}