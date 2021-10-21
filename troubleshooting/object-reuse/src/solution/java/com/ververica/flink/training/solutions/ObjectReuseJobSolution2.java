package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.util.Collector;

import com.ververica.flink.training.provided.GeoUtils2;
import com.ververica.flink.training.provided.MeanGauge;
import com.ververica.flink.training.provided.WeatherUtils;
import com.ververica.flink.training.solutions.immutable.ExtendedMeasurement;
import com.ververica.flink.training.solutions.immutable.Location;
import com.ververica.flink.training.solutions.immutable.MeasurementValue;
import com.ververica.flink.training.solutions.immutable.ObjectReuseExtendedMeasurementSource;
import com.ververica.flink.training.solutions.immutable.Sensor;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;

/**
 * Object reuse solution that builds on top of {@link ObjectReuseJobSolution1} and uses immutable
 * custom types for further performance improvements.
 */
public class ObjectReuseJobSolution2 {

    /**
     * Creates and starts the object reuse job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

        final boolean objectReuse = parameters.getBoolean("objectReuse", false);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        // Checkpointing Configuration
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4000);

        SingleOutputStreamOperator<ExtendedMeasurement> temperatureStream =
                env.addSource(new ObjectReuseExtendedMeasurementSource())
                        .name("FakeMeasurementSource")
                        .uid("FakeMeasurementSource")
                        .filter(
                                t ->
                                        t.getSensor()
                                                .getSensorType()
                                                .equals(Sensor.SensorType.Temperature))
                        .name("FilterTemperature")
                        .uid("FilterTemperature");

        // (1) stream with the temperature converted into local temperature units (°F in the US)
        temperatureStream
                .map(new ConvertToLocalTemperature())
                .name("ConvertToLocalTemperature")
                .uid("ConvertToLocalTemperature")
                .addSink(new DiscardingSink<>())
                .name("LocalizedTemperatureSink")
                .uid("LocalizedTemperatureSink")
                .disableChaining();

        // (2) stream with an (exponentially) moving average of the temperature (smoothens sensor
        //     measurements, variant A); then converted into local temperature units (°F in the US)
        temperatureStream
                .flatMap(new MovingAverageSensors())
                .name("MovingAverageTemperature")
                .uid("MovingAverageTemperature")
                .map(new ConvertToLocalTemperature())
                .name("ConvertToLocalAverageTemperature")
                .uid("ConvertToLocalAverageTemperature")
                .addSink(new DiscardingSink<>())
                .name("LocalizedAverageTemperatureSink")
                .uid("LocalizedAverageTemperatureSink")
                .disableChaining();

        env.execute(ObjectReuseJobSolution2.class.getSimpleName());
    }

    /**
     * Implements an exponentially moving average with a coefficient of <code>0.5</code>, i.e.
     *
     * <ul>
     *   <li><code>avg[0] = value[0]</code> (not forwarded to the next stream)
     *   <li><code>avg[i] = avg[i-1] * 0.5 + value[i] * 0.5</code> (for <code>i > 0</code>)
     * </ul>
     *
     * <p>See <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
     * https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average</a>
     */
    private static class MovingAverageSensors
            extends RichFlatMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
        private static final long serialVersionUID = 1L;

        private final Map<Sensor, Tuple2<FloatValue, DoubleValue>> lastAverage = new HashMap<>();

        @Override
        public void flatMap(ExtendedMeasurement value, Collector<ExtendedMeasurement> out) {
            Sensor sensor = value.getSensor();
            MeasurementValue measurement = value.getMeasurement();

            Tuple2<FloatValue, DoubleValue> last = lastAverage.get(sensor);
            if (last != null) {
                last.f0.setValue((last.f0.getValue() + measurement.getAccuracy()) / 2.0f);
                last.f1.setValue((last.f1.getValue() + measurement.getValue()) / 2.0);

                MeasurementValue averageMeasurement =
                        new MeasurementValue(
                                last.f1.getValue(), last.f0.getValue(), measurement.getTimestamp());
                ExtendedMeasurement forward =
                        new ExtendedMeasurement(
                                value.getSensor(), value.getLocation(), averageMeasurement);

                // do not forward the first value (it only stands alone)
                out.collect(forward);
            } else {
                lastAverage.put(
                        sensor,
                        Tuple2.of(
                                new FloatValue(measurement.getAccuracy()),
                                new DoubleValue(measurement.getValue())));
            }
        }
    }

    /**
     * Converts SI units to locale-dependent units, i.e. °C to °F for the US. Adds a custom metric
     * to report temperatures in the US.
     */
    private static class ConvertToLocalTemperature
            extends RichMapFunction<ExtendedMeasurement, ExtendedMeasurement> {
        private static final long serialVersionUID = 1L;

        private transient MeanGauge normalizedTemperatureUS;

        @Override
        public void open(final Configuration parameters) {
            normalizedTemperatureUS =
                    getRuntimeContext()
                            .getMetricGroup()
                            .gauge("normalizedTemperatureUSmean", new MeanGauge());
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge(
                            "normalizedTemperatureUSmin",
                            new MeanGauge.MinGauge(normalizedTemperatureUS));
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge(
                            "normalizedTemperatureUSmax",
                            new MeanGauge.MaxGauge(normalizedTemperatureUS));
        }

        @Override
        public ExtendedMeasurement map(ExtendedMeasurement value) {
            Location location = value.getLocation();
            if (GeoUtils2.isInUS(location.getLongitude(), location.getLatitude())) {
                MeasurementValue measurement = value.getMeasurement();
                double normalized = WeatherUtils.celciusToFahrenheit(measurement.getValue());

                MeasurementValue localizedMeasurement =
                        new MeasurementValue(
                                normalized, measurement.getAccuracy(), measurement.getTimestamp());

                normalizedTemperatureUS.addValue(normalized);
                return new ExtendedMeasurement(
                        value.getSensor(), value.getLocation(), localizedMeasurement);
            } else {
                return value;
            }
        }
    }
}
