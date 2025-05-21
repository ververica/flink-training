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
import com.ververica.flink.training.provided.OneMinuteWindowCountAggregator;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import com.ververica.flink.training.provided.SetTimeFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Return a new stream with the two longest transactions per a
 * configurable window. By "longest transaction" we mean a transaction's
 * duration, calculated as delta from the completed record to the
 * first record. This result is a KeyedWindowResult(transactionId, time, duration)
 */
public class BootcampWindowing3Workflow {

    protected DataStream<ShoppingCartRecord> cartStream;
    protected Sink<KeyedWindowResult> oneMinuteSink;
    protected Sink<WindowAllResult> fiveMinuteSink;
    protected Sink<KeyedWindowResult> longestTransactionsSink;
    protected int transactionWindowInMinutes = 5;

    public BootcampWindowing3Workflow() {
    }

    public BootcampWindowing3Workflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampWindowing3Workflow setOneMinuteSink(Sink<KeyedWindowResult> oneMinuteSink) {
        this.oneMinuteSink = oneMinuteSink;
        return this;
    }

    public BootcampWindowing3Workflow setFiveMinuteSink(Sink<WindowAllResult> fiveMinuteSink) {
        this.fiveMinuteSink = fiveMinuteSink;
        return this;
    }

    public BootcampWindowing3Workflow setLongestTransactionsSink(Sink<KeyedWindowResult> longestTransactionsSink) {
        this.longestTransactionsSink = longestTransactionsSink;
        return this;
    }

    public BootcampWindowing3Workflow setTransactionsWindowInMinutes(int transactionWindowInMinutes) {
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
                // TODO - make it so.
                // You'll first have to key by transactionId, and use a session window to find the first/
                // last transactions (and thus the duration) for completed transactions.
                // Then you'll have to use a windowAll with a tumbling time window, and an
                // aggregate function to keep track of the two longest transactions for each
                // window.
                // This is a placeholder map() call to get it to compile.
                .map(r -> new KeyedWindowResult(r.getTransactionId(), r.getTransactionTime(), 0L))
                .sinkTo(longestTransactionsSink);

    }

}