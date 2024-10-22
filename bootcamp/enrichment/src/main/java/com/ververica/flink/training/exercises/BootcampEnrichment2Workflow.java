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

import com.ververica.flink.training.common.ProductInfoRecord;
import com.ververica.flink.training.common.ProductRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.KeyedWindowDouble;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import com.ververica.flink.training.provided.SumWeightAggregator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Solution to the second exercise in the eCommerce enrichment lab.
 * 1. Convert shopping cart records into separate ProductRecords
 * 2. Join the product info stream with the product stream
 * 3. Add product information to the product records
 * 2. Key by country, window per minute
 * 3. Generate per-product/per-minute shipping weight
 */
public class BootcampEnrichment2Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected DataStream<ProductInfoRecord> productInfoStream;
    protected Sink<KeyedWindowDouble> resultSink;

    public BootcampEnrichment2Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampEnrichment2Workflow setProductInfoStream(DataStream<ProductInfoRecord> productInfoStream) {
        this.productInfoStream = productInfoStream;
        return this;
    }

    public BootcampEnrichment2Workflow setResultSink(Sink<KeyedWindowDouble> resultSink) {
        this.resultSink = resultSink;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(productInfoStream, "productInfoStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks, and filter out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Turn into a per-product stream
        DataStream<ProductRecord> productStream = filtered
                // Use a flatMap to convert one shopping cart into 1...N ProductRecords.
                .flatMap(new ExplodeShoppingCartFunction())
                .name("Explode shopping cart");

        // Assign timestamps & watermarks
        DataStream<ProductInfoRecord> watermarkedProduct = productInfoStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductInfoRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getInfoTime()));

        // Connect products with the product info stream, using product ID,
        // and enrich the product records. Both streams need to be keyed by the
        // product id before connecting. Then .process(custom KeyedCoProcessFunction())
        // can be used
        DataStream<ProductRecord> enrichedStream = productStream
                .keyBy(r -> r.getProductId())
                .connect(watermarkedProduct.keyBy(r -> r.getProductId()))
                // Use a KeyedCoProcessFunction to join the two streams of data.
                .process(new AddProductInfoFunction())
                .name("Enriched products");

        // Key by country, tumbling window per minute
        enrichedStream.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SumWeightAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }
}