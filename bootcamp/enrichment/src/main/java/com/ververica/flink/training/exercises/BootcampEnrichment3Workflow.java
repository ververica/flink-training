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

import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import com.ververica.flink.training.provided.SumDollarsAggregator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Solution to the first exercise in the eCommerce enrichment lab.
 * 1. Call API to get exchange rate - cache results
 * 2. Key by country, window per minute
 * 3. Generate per-country/per minute sales in US$
 */
public class BootcampEnrichment3Workflow extends BootcampEnrichment1Workflow {

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(resultSink, "resultSink must be set");

        // Assign timestamps & watermarks, and filter out pending carts
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());

        // Enrich by calculating US$ price to all cart items, summing total based on quantity,
        // and outputting Tuple2<country, usdEquivalent>
        DataStream<Tuple2<String, Double>> withUSPrices = filtered
                // We now use a caching version of the map function
                .map(new CalcTotalUSDollarPriceWithCacheFunction(startTime))
                .name("Calc US dollar price");

        // Key by country, tumbling window per minute
        withUSPrices.keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SumDollarsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
    }
}