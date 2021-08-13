package com.ververica.flink.training.solutions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static com.ververica.flink.training.common.EnvironmentUtils.createConfiguredEnvironment;
import static com.ververica.flink.training.common.EnvironmentUtils.isLocal;

/**
 * Solution 3 fixes the streaming job with slow checkpointing by sorting the stream based on event time
 * then pre-aggregation
 */
public class CheckpointingJobSolution3 {

    /**
     * Creates and starts the troubled streaming job.
	 *
	 * @throws Exception if the application is misconfigured or fails during job submission
     */
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = createConfiguredEnvironment(parameters);

		//Timing Configuration
		env.getConfig().setAutoWatermarkInterval(100);
		env.setBufferTimeout(10);

		//Checkpointing Configuration (use cluster-configs if not run locally)
		if (isLocal(parameters)) {
			env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
			env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
			env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(2));
		}

		DataStream<Tuple2<Measurement, Long>> sourceStream = env
				.addSource(SourceUtils.createFailureFreeFakeKafkaSource())
				.name("FakeKafkaSource")
				.uid("FakeKafkaSource")
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<FakeKafkaRecord>forBoundedOutOfOrderness(Duration.ofMillis(250))
								.withTimestampAssigner(
										(element, timestamp) -> element.getTimestamp())
								.withIdleness(Duration.ofSeconds(1)))
				.name("Watermarks")
				.uid("Watermarks")
				.flatMap(new MeasurementDeserializer())
				.name("Deserialization")
				.uid("Deserialization");

		DataStream<Tuple2<Measurement, Long>> sortedStream = sourceStream
				.keyBy(x -> x.f0.getSensorId())
				.process(new SortMeasurementFunction())
				.name("Sorting")
				.uid("Sorting");

		KeyedStream<Tuple2<Measurement, Long>, Integer> keyedSortedStream =
				DataStreamUtils.reinterpretAsKeyedStream(
						sortedStream,
						x -> x.f0.getSensorId());

		DataStream<WindowedMeasurements> aggregatedPerLocation = keyedSortedStream
				.window(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.MINUTES), Time.of(1, TimeUnit.SECONDS)))
				.aggregate(new MeasurementWindowAggregatingFunction(),
						new MeasurementWindowProcessFunction())
				.name("WindowedAggregationPerLocation")
				.uid("WindowedAggregationPerLocation");

		if (isLocal(parameters)) {
			aggregatedPerLocation.print()
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		} else {
			aggregatedPerLocation.addSink(new DiscardingSink<>())
					.name("NormalOutput")
					.uid("NormalOutput")
					.disableChaining();
		}

		env.execute(CheckpointingJobSolution3.class.getSimpleName());
	}

	public static class SortMeasurementFunction
			extends KeyedProcessFunction<Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

		private ListState<Tuple2<Measurement, Long>> listState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			ListStateDescriptor<Tuple2<Measurement, Long>> desc =
					new ListStateDescriptor<>(
							"events",
							Types.TUPLE(Types.POJO(Measurement.class), Types.LONG)
					);
			listState = getRuntimeContext().getListState(desc);
		}

		@Override
		public void processElement(Tuple2<Measurement, Long> value, Context ctx,
								   Collector<Tuple2<Measurement, Long>> out) throws Exception {
			TimerService timerService = ctx.timerService();

			if (ctx.timestamp() > timerService.currentWatermark()) {
				listState.add(value);
				timerService.registerEventTimeTimer(ctx.timestamp());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx,
							Collector<Tuple2<Measurement, Long>> out) throws Exception {

			ArrayList<Tuple2<Measurement, Long>> list = new ArrayList<>();
			listState.get().iterator().forEachRemaining(list::add);
			list.sort(new MeasurementByTimeComparator());

			Long watermark = ctx.timerService().currentWatermark();
			int index = 0;
			for (Tuple2<Measurement, Long> event : list) {
				if (event != null && event.f1 <= watermark) {
					out.collect(event);
					index++;
				} else {
					break;
				}
			}
			list.subList(0, index).clear();
			listState.update(list);
		}
	}

	public static class MeasurementWindowAggregatingFunction implements
			AggregateFunction<Tuple2<Measurement, Long>, Tuple3<Long, Double, Double>, Tuple2<Long, Double>> {
		private static final long serialVersionUID = 1;

		@Override
		public Tuple3<Long, Double, Double> createAccumulator() {
			/**
			 * f0: the total number of events
			 * f1: the total differences summed up in the event time order
			 * f2: the value of the previous measurement
			 */
			return new Tuple3<Long, Double, Double>(0L, 0.0, 0.0);
		}

		@Override
		public Tuple3<Long, Double, Double> add(
				final Tuple2<Measurement, Long> record,
				final Tuple3<Long, Double, Double> aggregate) {

			if (aggregate.f0 > 0) {
				aggregate.f1 += record.f0.getValue() - aggregate.f2;
			}
			aggregate.f0++;
			aggregate.f2 = record.f0.getValue();
			return aggregate;
		}

		@Override
		public Tuple2<Long, Double> getResult(final Tuple3<Long, Double, Double> windowedMeasurements) {
			return new Tuple2<>(windowedMeasurements.f0, windowedMeasurements.f1);
		}

		@Override
		public Tuple3<Long, Double, Double> merge(
				final Tuple3<Long, Double, Double> agg1,
				final Tuple3<Long, Double, Double> agg2) {
			// not needed in this case
			return null;
		}
	}

	/**
	 * Deserializes the JSON Kafka message.
	 */
	public static class MeasurementDeserializer extends
			RichFlatMapFunction<FakeKafkaRecord, Tuple2<Measurement, Long>> {
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
		public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Tuple2<Measurement, Long>> out) {
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

	private static class MeasurementByTimeComparator implements Comparator<Tuple2<Measurement, Long>> {
		@Override
		public int compare(Tuple2<Measurement, Long> o1, Tuple2<Measurement, Long> o2) {
			return Long.compare(o1.f1, o2.f1);
		}
	}

	/**
	 * Calculates data for retrieving the average temperature difference between two sensor readings
	 * (in event-time order!).
	 */
	public static class MeasurementWindowProcessFunction
			extends
			ProcessWindowFunction<Tuple2<Long, Double>, WindowedMeasurements, Integer, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

		private transient DescriptiveStatisticsHistogram eventTimeLag;

		@Override
		public void process(
				final Integer sensorId,
				final Context context,
				final Iterable<Tuple2<Long, Double>> input,
				final Collector<WindowedMeasurements> out) {

			final TimeWindow window = context.window();
			Tuple2<Long, Double> result = input.iterator().next();
			WindowedMeasurements windowedMeasurements = new WindowedMeasurements();
			windowedMeasurements.setEventsPerWindow(result.f0);
			windowedMeasurements.setSumPerWindow(result.f1);
			windowedMeasurements.setWindow(window);
			windowedMeasurements.setLocation(sensorId.toString());

			eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
			out.collect(windowedMeasurements);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			eventTimeLag = getRuntimeContext().getMetricGroup().histogram("eventTimeLag",
					new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
		}
	}

	private static ObjectMapper createObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return objectMapper;
	}
}
