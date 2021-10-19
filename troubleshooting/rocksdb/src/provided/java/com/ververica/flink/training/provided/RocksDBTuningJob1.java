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
import org.apache.flink.util.Collector;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.Measurement;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/** RocksDB Training job. */
@DoNotChangeThis
public class RocksDBTuningJob1 {

    /**
     * Creates and starts the troubled RocksDB job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        int numSensors = parameters.getInt("numSensors", 100_000);
        int payloadWords = parameters.getInt("payloadWords", 1_000);
        int lastNTimestamps = parameters.getInt("lastNTimestamps", 5);
        if (lastNTimestamps < 1) {
            throw new IllegalArgumentException(
                    "Must at least use one event for computing the result");
        }

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
                                                Duration.ofSeconds(10))
                                        .withTimestampAssigner((element, timestamp) -> element.f1)
                                        .withIdleness(Duration.ofSeconds(1)))
                        .name("Watermarks")
                        .uid("Watermarks");

        sourceStream
                .keyBy(x -> x.f0.getSensorId())
                .process(new MyProcessFunction1(lastNTimestamps))
                .name("MyProcessFunction1")
                .uid("MyProcessFunction1")
                .addSink(new DiscardingSink<>())
                .name("DiscardingSink")
                .uid("DiscardingSink")
                .disableChaining();

        env.execute(RocksDBTuningJob1.class.getSimpleName());
    }

    private static class MyProcessFunction1
            extends KeyedProcessFunction<
                    Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private static final long serialVersionUID = 1L;

        private final int lastNTimestamps;

        private transient MapState<Long, Measurement> eventsBuffer;

        private MyProcessFunction1(int lastNTimestamps) {
            this.lastNTimestamps = lastNTimestamps;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            MapStateDescriptor<Long, Measurement> eventsBufferDescriptor =
                    new MapStateDescriptor<>("eventsBuffer", Long.class, Measurement.class);

            eventsBuffer = getRuntimeContext().getMapState(eventsBufferDescriptor);
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
                eventsBuffer.put(currentTimestamp, value.f0);

                timerService.registerEventTimeTimer(currentTimestamp); // timer to process
                timerService.registerEventTimeTimer(
                        currentTimestamp + lastNTimestamps); // timer to cleanup
            }
            // ignore late events
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>>
                                .OnTimerContext
                        ctx,
                Collector<Tuple2<Measurement, Long>> out)
                throws Exception {
            super.onTimer(timestamp, ctx, out);
            Measurement measurementFromTimer = eventsBuffer.get(timestamp);
            if (measurementFromTimer == null) {
                // timer only for cleanup
                eventsBuffer.remove(timestamp - lastNTimestamps);
                return;
            }

            // cleanup or processing timer?
            double aggValue = 0;
            double decayingFactor = 0.5;
            boolean cleanupOldest = true;
            for (int i = lastNTimestamps; i >= 1; --i) {
                Measurement measurement = eventsBuffer.get(timestamp - i);
                if (i == lastNTimestamps) {
                    cleanupOldest = measurement != null;
                }
                aggValue =
                        aggValue * decayingFactor
                                + (measurement != null ? measurement.getValue() : 0);
            }
            aggValue = aggValue * decayingFactor + measurementFromTimer.getValue();
            out.collect(
                    Tuple2.of(
                            new Measurement(
                                    measurementFromTimer.getSensorId(),
                                    aggValue,
                                    measurementFromTimer.getLocation(),
                                    measurementFromTimer.getMeasurementInformation()),
                            timestamp));

            // this timer may be used both for processing and cleanup
            if (cleanupOldest) {
                eventsBuffer.remove(timestamp - lastNTimestamps);
            }
        }
    }
}
