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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.Measurement;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * RocksDB Training job.
 * <p>
 * This job outputs the latest measurements in windows that are calculated based on sensor ids.
 * */
@DoNotChangeThis
public class RocksDBTuningJob2 {

    /**
     * Creates and starts the troubled RocksDB job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        int numSensors = parameters.getInt("numSensors", 10_000_000);
        int payloadWords = parameters.getInt("payloadWords", 200);
        Duration windowSize = Duration.parse(parameters.get("windowSize", "PT10M"));
        boolean unalignedWindows = parameters.getBoolean("unalignedWindows", true);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

        // Checkpointing Configuration (use cluster-configs if not run locally)
        if (isLocal(parameters)) {
            env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
            env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(2));
        }

        DataStream<Tuple2<Measurement, Long>> sourceStream =
                env.addSource(new RocksDBTuningMeasurementSource(numSensors, payloadWords))
                        .name("FakeKafkaSource")
                        .uid("FakeKafkaSource")
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple2<Measurement, Long>>forBoundedOutOfOrderness(
                                                Duration.ofMillis(250))
                                        .withTimestampAssigner((element, timestamp) -> element.f1)
                                        .withIdleness(Duration.ofSeconds(1)))
                        .name("Watermarks")
                        .uid("Watermarks");

        sourceStream
                .keyBy(x -> x.f0.getSensorId())
                .process(new MyProcessFunction1(windowSize, numSensors, unalignedWindows))
                .name("MyProcessFunction1")
                .uid("MyProcessFunction1")
                .addSink(new DiscardingSink<>())
                .name("DiscardingSink")
                .uid("DiscardingSink")
                .disableChaining();

        env.execute(RocksDBTuningJob2.class.getSimpleName());
    }

    private static class MyProcessFunction1
            extends KeyedProcessFunction<
                    Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private static final long serialVersionUID = 1L;

        private final long windowSizeMs;
        private final int numSensors;
        private final boolean unalignedWindows;

        private transient MapState<Long, Measurement> lastValuePerWindow;

        private MyProcessFunction1(Duration windowSize, int numSensors, boolean unalignedWindows) {
            this.windowSizeMs = windowSize.toMillis();
            this.numSensors = numSensors;
            this.unalignedWindows = unalignedWindows;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            MapStateDescriptor<Long, Measurement> lastValuesDescriptor =
                    new MapStateDescriptor<>("lastValuePerWindow", Long.class, Measurement.class);

            lastValuePerWindow = getRuntimeContext().getMapState(lastValuesDescriptor);
        }

        @Override
        public void processElement(
                Tuple2<Measurement, Long> value,
                KeyedProcessFunction<Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>>
                                .Context
                        ctx,
                Collector<Tuple2<Measurement, Long>> out)
                throws Exception {
            TimerService timerService = ctx.timerService();
            Long currentTimestamp = ctx.timestamp();

            if (currentTimestamp > timerService.currentWatermark()) {
                long window = assignWindow(value, ctx.getCurrentKey());
                lastValuePerWindow.put(window, value.f0);

                timerService.registerEventTimeTimer(window); // time to process
            }
            // ignore late events
        }

        private long assignWindow(Tuple2<Measurement, Long> value, Integer currentKey) {
            long offset = getOffset(currentKey);
            long start = TimeWindow.getWindowStartWithOffset(value.f1, offset, windowSizeMs);
            return start + windowSizeMs;
        }

        private long getOffset(Integer currentKey) {
            if (unalignedWindows) {
                return Math.min(
                        windowSizeMs, (long) (currentKey * ((double) windowSizeMs / numSensors)));
            }
            return 0;
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>>
                                .OnTimerContext
                        ctx,
                Collector<Tuple2<Measurement, Long>> out)
                throws Exception {
            // timer to process an event
            out.collect(Tuple2.of(lastValuePerWindow.get(timestamp), timestamp));
            lastValuePerWindow.remove(timestamp);
        }
    }
}
