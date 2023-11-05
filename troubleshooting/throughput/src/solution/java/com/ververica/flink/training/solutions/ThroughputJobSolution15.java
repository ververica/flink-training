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

package com.ververica.flink.training.solutions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.Measurement;
import com.ververica.flink.training.common.SourceUtils;
import com.ververica.flink.training.common.WindowedMeasurements;
import com.ververica.flink.training.exercises.WindowedMeasurementsForArea;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/** Streaming job evolved by using custom Kryo serializer for WindowedMeasurementsForArea */
public class ThroughputJobSolution15 {

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

        // Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        // Register serialized types
        env.registerType(Measurement.class);
        env.registerType(WindowedMeasurements.class);
        env.registerType(WindowedMeasurementsForArea.class);

        env.getConfig().registerTypeWithKryoSerializer(WindowedMeasurements.class,
                WindowedMeasurementsSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(WindowedMeasurementsForArea.class,
                WindowedMeasurementsForAreaSerializer.class);

        DataStream<Measurement> sourceStream =
                env.addSource(SourceUtils.createFakeKafkaSource())
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
                        .keyBy(Measurement::getLocation)
                        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .aggregate(
                                new MeasurementWindowAggregatingPerLocation(),
                                new MeasurementWindowProcessFunction())
                        .name("WindowedAggregationPerLocation")
                        .uid("WindowedAggregationPerLocation");

        DataStream<WindowedMeasurementsForArea> aggregatedPerArea =
                aggregatedPerLocation
                        .keyBy(m -> WindowedMeasurementsForArea.getArea(m.getLocation()))
                        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .aggregate(new MeasurementWindowAggregatingPerArea())
                        .name("WindowedAggregationPerArea")
                        .uid("WindowedAggregationPerArea");

        if (isLocal(parameters)) {
            aggregatedPerLocation
                    .addSink(new DiscardingSink<>())
                    .name("OutputPerLocation")
                    .uid("OutputPerLocation")
                    .disableChaining();
            aggregatedPerArea.print().name("OutputPerArea").uid("OutputPerArea").disableChaining();
        } else {
            aggregatedPerLocation
                    .addSink(new DiscardingSink<>())
                    .name("OutputPerLocation")
                    .uid("OutputPerLocation")
                    .disableChaining();
            aggregatedPerArea
                    .addSink(new DiscardingSink<>())
                    .name("OutputPerArea")
                    .uid("OutputPerArea")
                    .disableChaining();
        }

        env.execute(ThroughputJobSolution15.class.getSimpleName());
    }

    /** Deserializes the JSON Kafka message. */
    public static class MeasurementDeserializer
            extends RichFlatMapFunction<FakeKafkaRecord, Measurement> {
        private static final long serialVersionUID = 2L;

        private Counter numInvalidRecords;
        private transient ObjectMapper instance;

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            numInvalidRecords = getRuntimeContext().getMetricGroup().counter("numInvalidRecords");
            instance = createObjectMapper();
        }

        @Override
        public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Measurement> out) {
            try {
                out.collect(deserialize(kafkaRecord.getValue()));
            } catch (IOException e) {
                numInvalidRecords.inc();
            }
        }

        private Measurement deserialize(final byte[] bytes) throws IOException {
            return instance.readValue(bytes, Measurement.class);
        }
    }

    public static class MeasurementWindowProcessFunction
            extends ProcessWindowFunction<
                    WindowedMeasurements, WindowedMeasurements, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

        private transient DescriptiveStatisticsHistogram eventTimeLag;

        @Override
        public void process(
                final String location,
                final Context context,
                final Iterable<WindowedMeasurements> input,
                final Collector<WindowedMeasurements> out) {

            // Windows with pre-aggregation only forward the final aggregate to the WindowFunction
            WindowedMeasurements aggregate = input.iterator().next();

            final TimeWindow window = context.window();
            aggregate.setWindow(window);
            aggregate.setLocation(location);

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

    public static class MeasurementWindowAggregatingPerLocation
            implements AggregateFunction<Measurement, WindowedMeasurements, WindowedMeasurements> {

        private static final long serialVersionUID = 2L;

        @Override
        public WindowedMeasurements add(
                final Measurement record, final WindowedMeasurements aggregate) {
            aggregate.addMeasurement(record);
            return aggregate;
        }

        @Override
        public WindowedMeasurements createAccumulator() {
            return new WindowedMeasurements();
        }

        @Override
        public WindowedMeasurements getResult(final WindowedMeasurements windowedMeasurements) {
            return windowedMeasurements;
        }

        @Override
        public WindowedMeasurements merge(
                final WindowedMeasurements agg1, final WindowedMeasurements agg2) {
            agg2.setEventsPerWindow(agg1.getEventsPerWindow() + agg2.getEventsPerWindow());
            agg2.setSumPerWindow(agg1.getSumPerWindow() + agg2.getSumPerWindow());
            return agg2;
        }
    }

    public static class MeasurementWindowAggregatingPerArea
            implements AggregateFunction<
                    WindowedMeasurements,
                    WindowedMeasurementsForArea,
                    WindowedMeasurementsForArea> {

        private static final long serialVersionUID = 1L;

        @Override
        public WindowedMeasurementsForArea add(
                final WindowedMeasurements value, final WindowedMeasurementsForArea aggregate) {
            aggregate.addMeasurement(value);
            return aggregate;
        }

        @Override
        public WindowedMeasurementsForArea getResult(
                WindowedMeasurementsForArea windowedMeasurements) {
            String aLocation = windowedMeasurements.getLocations().get(0);
            windowedMeasurements.setArea(WindowedMeasurementsForArea.getArea(aLocation));
            return windowedMeasurements;
        }

        @Override
        public WindowedMeasurementsForArea createAccumulator() {
            return new WindowedMeasurementsForArea();
        }

        @Override
        public WindowedMeasurementsForArea merge(
                final WindowedMeasurementsForArea agg1, final WindowedMeasurementsForArea agg2) {
            agg2.setEventsPerWindow(agg1.getEventsPerWindow() + agg2.getEventsPerWindow());
            agg2.setSumPerWindow(agg1.getSumPerWindow() + agg2.getSumPerWindow());
            agg2.addAllLocations(agg1.getLocations());
            return agg2;
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }

    public static class WindowedMeasurementsSerializer extends Serializer<WindowedMeasurements> {

        @Override
        public void write(Kryo kryo, Output output, WindowedMeasurements value) {
            output.writeLong(value.getWindowStart());
            output.writeLong(value.getWindowEnd());
            output.writeString(value.getLocation());
            output.writeLong(value.getEventsPerWindow());
            output.writeDouble(value.getSumPerWindow());
        }

        @Override
        public WindowedMeasurements read(Kryo kryo, Input input, Class<WindowedMeasurements> type) {
            WindowedMeasurements result = new WindowedMeasurements();
            result.setWindowStart(input.readLong());
            result.setWindowEnd(input.readLong());
            result.setLocation(input.readString());
            result.setEventsPerWindow(input.readLong());
            result.setSumPerWindow(input.readDouble());
            return result;
        }
    }

    public static class WindowedMeasurementsForAreaSerializer extends Serializer<WindowedMeasurementsForArea> {

        @Override
        public void write(Kryo kryo, Output output, WindowedMeasurementsForArea value) {
            output.writeLong(value.getWindowStart());
            output.writeLong(value.getWindowEnd());
            output.writeString(value.getArea());
            output.writeInt(value.getLocations().size());
            for (String location : value.getLocations()) {
                output.writeString(location);
            }
            output.writeLong(value.getEventsPerWindow());
            output.writeDouble(value.getSumPerWindow());
        }

        @Override
        public WindowedMeasurementsForArea read(Kryo kryo, Input input, Class<WindowedMeasurementsForArea> type) {
            WindowedMeasurementsForArea result = new WindowedMeasurementsForArea();
            result.setWindowStart(input.readLong());
            result.setWindowEnd(input.readLong());
            result.setArea(input.readString());

            final int numLocations = input.readInt();
            for (int i = 0; i < numLocations; i++) {
                result.addLocation(input.readString());
            }

            result.setEventsPerWindow(input.readLong());
            result.setSumPerWindow(input.readDouble());
            return result;
        }
    }
}
