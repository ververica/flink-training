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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.Measurement;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.common.WindowedMeasurements;

import java.io.IOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Streaming job that isn't performing as expected. Checkpointing is slow.
 * <p>
 * This job read sensor measurements event from a Kafka source (faked), deserialize them, and calculate the average
 * temperature difference between two sensor readings in event-time order using a window operator.
 * */
public class CheckpointingJob {

    /**
     * Creates and starts the troubled streaming job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

        // Timing Configuration
        env.getConfig().setAutoWatermarkInterval(100);
        env.setBufferTimeout(10);

        // Checkpointing Configuration (use cluster-configs if not run locally)
        if (isLocal(parameters)) {
            env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
            env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(2));
        }

        DataStream<Tuple2<Measurement, Long>> sourceStream =
                env.addSource(SourceUtils.createFailureFreeFakeKafkaSource())
                        .name("FakeKafkaSource")
                        .uid("FakeKafkaSource")
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<FakeKafkaRecord>forBoundedOutOfOrderness(
                                                Duration.ofMillis(250))
                                        .withTimestampAssigner(
                                                (element, timestamp) -> element.getTimestamp())
                                        .withIdleness(Duration.ofSeconds(1)))
                        .name("Watermarks")
                        .uid("Watermarks")
                        .flatMap(new MeasurementDeserializer())
                        .name("Deserialization")
                        .uid("Deserialization");

        DataStream<WindowedMeasurements> aggregatedPerLocation =
                sourceStream
                        .keyBy(x -> x.f0.getSensorId())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.MINUTES), Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(
                                new MeasurementWindowAggregatingFunction(),
                                new MeasurementWindowProcessFunction())
                        .name("WindowedAggregationPerLocation")
                        .uid("WindowedAggregationPerLocation");

        if (isLocal(parameters)) {
            aggregatedPerLocation
                    .print()
                    .name("NormalOutput")
                    .uid("NormalOutput")
                    .disableChaining();
        } else {
            aggregatedPerLocation
                    .addSink(new DiscardingSink<>())
                    .name("NormalOutput")
                    .uid("NormalOutput")
                    .disableChaining();
        }

        env.execute(CheckpointingJob.class.getSimpleName());
    }

    /** Deserializes the JSON Kafka message. */
    public static class MeasurementDeserializer
            extends RichFlatMapFunction<FakeKafkaRecord, Tuple2<Measurement, Long>> {
        private static final long serialVersionUID = 3L;

        private Counter numInvalidRecords;
        private transient ObjectMapper instance;

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
            instance = createObjectMapper();
        }

        @Override
        public void flatMap(
                final FakeKafkaRecord kafkaRecord, final Collector<Tuple2<Measurement, Long>> out) {
            final Measurement node;
            try {
                node = deserialize(kafkaRecord.getValue());
            } catch (IOException e) {
                numInvalidRecords.inc();
                return;
            }
            out.collect(Tuple2.of(node, kafkaRecord.getTimestamp()));
        }

        private Measurement deserialize(final byte[] bytes) throws IOException {
            return instance.readValue(bytes, Measurement.class);
        }
    }

    private static class MeasurementByTimeComparator
            implements Comparator<Tuple2<Measurement, Long>> {
        @Override
        public int compare(Tuple2<Measurement, Long> o1, Tuple2<Measurement, Long> o2) {
            return Long.compare(o1.f1, o2.f1);
        }
    }

    /**
     * Calculates data for retrieving the average temperature difference between two sensor readings
     * (in event-time order!).
     */
    public static class MeasurementWindowAggregatingFunction
            implements AggregateFunction<
                    Tuple2<Measurement, Long>,
                    PriorityQueue<Tuple2<Measurement, Long>>,
                    WindowedMeasurements> {
        private static final long serialVersionUID = 1;

        @Override
        public PriorityQueue<Tuple2<Measurement, Long>> createAccumulator() {
            // note: "Comparator.comparingLong(o -> o.f1)" cannot be used with Kryo
            return new PriorityQueue<>(new MeasurementByTimeComparator());
        }

        @Override
        public PriorityQueue<Tuple2<Measurement, Long>> add(
                final Tuple2<Measurement, Long> record,
                final PriorityQueue<Tuple2<Measurement, Long>> aggregate) {
            aggregate.add(record);
            //			System.out.println("Elements for " + record.f0.getSensorId() + ": " +
            // aggregate.size());
            return aggregate;
        }

        @Override
        public WindowedMeasurements getResult(
                final PriorityQueue<Tuple2<Measurement, Long>> windowedMeasurements) {
            long eventsPerWindow = 0L;
            double sumPerWindow = 0.0;

            Measurement previous = null;
            while (!windowedMeasurements.isEmpty()) {
                Tuple2<Measurement, Long> measurement = windowedMeasurements.poll();
                ++eventsPerWindow;
                if (previous != null) {
                    double diffPerMeasurement = measurement.f0.getValue() - previous.getValue();
                    sumPerWindow += diffPerMeasurement * diffPerMeasurement;
                }
                previous = measurement.f0;
            }

            WindowedMeasurements result = new WindowedMeasurements();
            result.setEventsPerWindow(eventsPerWindow);
            result.setSumPerWindow(sumPerWindow);

            return result;
        }

        @Override
        public PriorityQueue<Tuple2<Measurement, Long>> merge(
                final PriorityQueue<Tuple2<Measurement, Long>> agg1,
                final PriorityQueue<Tuple2<Measurement, Long>> agg2) {
            agg1.addAll(agg2);
            return agg1;
        }
    }

    public static class MeasurementWindowProcessFunction
            extends ProcessWindowFunction<
                    WindowedMeasurements, WindowedMeasurements, Integer, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

        private transient DescriptiveStatisticsHistogram eventTimeLag;

        @Override
        public void process(
                final Integer sensorId,
                final Context context,
                final Iterable<WindowedMeasurements> input,
                final Collector<WindowedMeasurements> out) {

            // Windows with pre-aggregation only forward the final result to the WindowFunction
            WindowedMeasurements aggregate = input.iterator().next();

            final TimeWindow window = context.window();
            aggregate.setWindow(window);
            aggregate.setLocation(sensorId.toString());

            eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
            out.collect(aggregate);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            eventTimeLag =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "eventTimeLag",
                                    new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }
}
