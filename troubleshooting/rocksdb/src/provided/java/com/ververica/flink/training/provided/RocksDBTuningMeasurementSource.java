package com.ververica.flink.training.provided;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.github.javafaker.Faker;
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.Measurement;

import java.util.List;
import java.util.SplittableRandom;

import static com.ververica.flink.training.common.SourceUtils.readLocationsFromFile;

/** Artificial source for sensor measurements. */
@SuppressWarnings("WeakerAccess")
@DoNotChangeThis
public class RocksDBTuningMeasurementSource
        extends RichParallelSourceFunction<Tuple2<Measurement, Long>> {

    private static final long serialVersionUID = 1L;

    private final int maxSensorId;
    private final int payloadWords;

    private volatile boolean running = true;

    private transient List<String> locations;
    private transient String randomMeasurementInfoSample;

    /**
     * Creates the source with a given number of sensors to choose from and having <code>
     * payloadWords</code> words to choose from in the payload field of {@link Measurement}.
     */
    public RocksDBTuningMeasurementSource(int maxSensorId, int payloadWords) {
        if (maxSensorId <= 0) {
            throw new IllegalArgumentException("maxSensorId must be greater than 0");
        }
        this.maxSensorId = maxSensorId;
        if (payloadWords < 0) {
            throw new IllegalArgumentException("payloadWords must be greater than or equal to 0");
        }
        this.payloadWords = payloadWords;
    }

    @Override
    public void open(final Configuration parameters) {
        initSensors();
    }

    @Override
    public void run(SourceContext<Tuple2<Measurement, Long>> ctx) {
        final SplittableRandom rnd = new SplittableRandom();
        final Object lock = ctx.getCheckpointLock();

        while (running) {
            Tuple2<Measurement, Long> event = randomEvent(rnd);

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (lock) {
                ctx.collect(event);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /** Creates sensor metadata that this source instance will work with. */
    private void initSensors() {
        locations = readLocationsFromFile();

        final Faker faker = new Faker();
        if (payloadWords == 0) {
            randomMeasurementInfoSample = "";
        } else {
            randomMeasurementInfoSample = faker.lorem().sentence(payloadWords * 2, 0);
        }
    }

    /** Creates a randomized sensor value. */
    private Tuple2<Measurement, Long> randomEvent(SplittableRandom rnd) {
        int sensorId = rnd.nextInt(maxSensorId);
        String location = locations.get(rnd.nextInt(locations.size()));

        long timestamp = System.currentTimeMillis();

        return Tuple2.of(
                new Measurement(
                        sensorId,
                        rnd.nextDouble() * 100,
                        location,
                        payloadWords == 0
                                ? ""
                                : randomMeasurementInfoSample.substring(
                                        rnd.nextInt(randomMeasurementInfoSample.length() / 2),
                                        randomMeasurementInfoSample.length() / 2)),
                timestamp);
    }
}
