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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

/**
 * The Windowing exercise from Ververica's Flink bootcamp training.
 *
 * <p>The goal of this exercise is to filter a data stream of eCommerce shopping cart records
 * to keep only records for completed transactions, and then calculate per-country/per-minute
 * aggregations of shopping cart item counts.
 */
public class BootcampWindowing1Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<KeyedWindowResult> resultSink;

    public BootcampWindowing1Workflow() {
    }

    public BootcampWindowing1Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampWindowing1Workflow setResultSink(Sink<KeyedWindowResult> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // TODO - add watermarks to cartStream, using the .assignTimestampsAndWatermarks() call and
        // the appropriate WatermarkStrategy.<ShoppingCartRecord> method

        // TODO - filter out transactions out that are NOT completed.

        // TODO - key by the country, then create 1 minute tumbling event time windows

        // TODO - aggregate the count of items using CountCartItemsAggregator, and
        // SetKeyAndTimeFunction to set the desired KeyedWindowResult(country, window start time, count)

        // ==================================================================
        // Placeholder to get code to compile
        // ==================================================================

        cartStream.map(r -> new KeyedWindowResult("US", 0L, 0))
                .sinkTo(resultSink);
    }
}