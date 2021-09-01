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

package org.apache.flink.training.solutions.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;

/**
 * Reference implementation for the Hourly Tips exercise from the Flink training, using the
 * Table/SQL API.
 *
 * <p>The goal of this exercise is to find the driver earning the most in tips in each hour.
 */
public class HourlyTipsTableSolution {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsTableSolution(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsTableSolution job =
                new HourlyTipsTableSolution(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environments
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // start the data generator
        DataStream<TaxiFare> fareStream = env.addSource(source);

        // convert the DataStream to a Table
        Schema fareSchema =
                Schema.newBuilder()
                        .column("driverId", "BIGINT")
                        .column("tip", "FLOAT")
                        .column("startTime", "TIMESTAMP_LTZ(3)")
                        .watermark("startTime", "startTime")
                        .build();
        tableEnv.createTemporaryView("fares", fareStream, fareSchema);

        // find the driver with the highest sum of tips for each hour
        Table hourlyMax =
                tableEnv.sqlQuery(
                        "SELECT 1000 * UNIX_TIMESTAMP(CAST(window_end AS STRING)) AS window_end, driverId, sumOfTips"
                                + "  FROM ("
                                + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end"
                                + "        ORDER BY sumOfTips DESC) AS rownum"
                                + "    FROM ("
                                + "      SELECT window_start, window_end, driverId, SUM(tip) AS sumOfTips"
                                + "      FROM TABLE("
                                + "        TUMBLE(TABLE fares, DESCRIPTOR(startTime), INTERVAL '1' HOUR))"
                                + "      GROUP BY window_start, window_end, driverId"
                                + "    )"
                                + "  ) WHERE rownum <= 1");

        // convert the query's results into a DataStream of the type expected by the tests
        DataStream<Tuple3<Long, Long, Float>> resultsAsStreamOfTuples =
                tableEnv.toDataStream(hourlyMax)
                        .map(
                                row ->
                                        new Tuple3<>(
                                                row.<Long>getFieldAs("window_end"),
                                                row.<Long>getFieldAs("driverId"),
                                                row.<Float>getFieldAs("sumOfTips")))
                        .returns(TypeInformation.of(new TypeHint<Tuple3<Long, Long, Float>>() {}));

        resultsAsStreamOfTuples.addSink(sink);

        // execute the pipeline
        return env.execute("Hourly Tips");
    }
}
