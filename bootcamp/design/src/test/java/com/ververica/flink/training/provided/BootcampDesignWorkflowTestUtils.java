package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.exercises.BootcampDesignAnalyticsWorkflow;
import com.ververica.flink.training.exercises.BootcampDesignDetectionWorkflow;
import com.ververica.flink.training.solutions.BootcampDesignAnalyticsSolutionWorkflow;
import com.ververica.flink.training.solutions.BootcampDesignDetectionSolutionWorkflow;
import com.ververica.flink.training.solutions.BootcampDesignSolutionTest;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ververica.flink.training.common.BootcampTestUtils.START_TIME;
import static com.ververica.flink.training.common.BootcampTestUtils.createShoppingCart;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampDesignWorkflowTestUtils {

    public static List<ShoppingCartRecord> makeCartRecords() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);
        List<ShoppingCartRecord> records = new ArrayList<>();

        // Add a record that is abandoned
        ShoppingCartRecord c11 = createShoppingCart(generator, "US");
        c11.setCustomerId("c1");
        c11.setTransactionId("r1");
        c11.getItems().get(0).setProductId("p1");
        records.add(c11);

        // Add another record for this same customer, but with a different
        // transaction id, and more items in the cart.
        ShoppingCartRecord c12 = createShoppingCart(generator, "US");
        c12.setTransactionId("t2");
        c12.setCustomerId("c1");
        c12.setTransactionTime(START_TIME + 500);
        c12.getItems().get(0).setProductId("p2");
        c12.getItems().add(generator.createCartItem("US"));
        records.add(c12);

        return records;
    }

    public static void testBootcampDesignWorkflows(BootcampDesignAnalyticsWorkflow analyticsWorkflow,
                                                   BootcampDesignDetectionWorkflow detectionWorkflow) throws Exception {
        final StreamExecutionEnvironment env1 = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        final StreamExecutionEnvironment env2 = FlinkClusterUtils.createConfiguredTestEnvironment(2);

        List<ShoppingCartRecord> carts = BootcampDesignWorkflowTestUtils.makeCartRecords();

        AnalyticsSink analyticsSink = new AnalyticsSink();
        AbandonedSinkAndSource abandonedSinkAndSource = new AbandonedSinkAndSource();
        BadCustomerSink badCustomerSink = new BadCustomerSink();

        analyticsWorkflow
                .setCartStream(env1.fromData(carts).setParallelism(1))
                .setAbandonedSink(abandonedSinkAndSource)
                .setAnalyticsSink(analyticsSink)
                .build();

        DataStream<AbandonedCartItem> abandonedStream = env2.addSource(new AbandonedSinkAndSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AbandonedCartItem>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        detectionWorkflow
                .setAbandonedStream(abandonedStream)
                .setBadCustomerSink(badCustomerSink)
                .setMaxAbandonedPerMinute(1)
                .build();

        JobClient client1 = env1.executeAsync("BootcampDesignAnalyticsSolutionWorkflow");
        JobClient client2 = env2.executeAsync("BootcampDesignDetectionSolutionWorkflow");

        // Wait until the first job completes, then tell our source to finish when it's out
        // of data. This will trigger a max watermark, which causes window results to be
        // emitted (otherwise we have the idle source problem forever).
        while (!client1.getJobExecutionResult().isDone()) {
            Thread.sleep(10L);
        }

        abandonedSinkAndSource.stopSourceWhenEmpty();

        while (!client2.getJobExecutionResult().isDone()) {
            Thread.sleep(10L);
        }

        validateAnalyticsResults(analyticsSink.getSink());
        validateBadCustomerResults(badCustomerSink.getSink());
    }

    private static class AbandonedSinkAndSource implements Sink<AbandonedCartItem>, SourceFunction<AbandonedCartItem> {

        private static List<AbandonedCartItem> QUEUE = Collections.synchronizedList(new ArrayList<AbandonedCartItem>());
        private static AtomicInteger QUEUE_READ_POS = new AtomicInteger(0);

        private static AtomicBoolean TERMINATE_SOURCE = new AtomicBoolean(false);

        private transient boolean keepRunning = false;

        public AbandonedSinkAndSource() {
            QUEUE.clear();
            QUEUE_READ_POS.set(0);
            TERMINATE_SOURCE.set(false);
        }

        public List<AbandonedCartItem> getSink() {
            return QUEUE;
        }

        public void stopSourceWhenEmpty() {
            TERMINATE_SOURCE.set(true);
        }

        @Override
        public void run(SourceContext<AbandonedCartItem> ctx) throws Exception {
            keepRunning = true;
            while (keepRunning) {
                synchronized(QUEUE_READ_POS) {
                    if (QUEUE_READ_POS.get() < QUEUE.size()) {
                        ctx.collect(QUEUE.get(QUEUE_READ_POS.getAndIncrement()));
                    } else if (TERMINATE_SOURCE.get()) {
                        keepRunning = false;
                    }
                }

                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            keepRunning = false;
        }

        @Override
        public SinkWriter<AbandonedCartItem> createWriter(InitContext context) throws IOException {
            return new SinkWriter<AbandonedCartItem>() {

                @Override
                public void close() throws Exception {
                }

                @Override
                public void flush(boolean arg0) throws IOException, InterruptedException {
                }

                @Override
                public void write(AbandonedCartItem element, Context ctx) throws IOException, InterruptedException {
                    QUEUE.add(element);
                }
            };
        }
    }

    public static void validateAnalyticsResults(Collection<KeyedWindowResult> result) {
        assertThat(result).containsExactlyInAnyOrder(
                new KeyedWindowResult("c1", START_TIME, 2));
    }

    public static void validateBadCustomerResults(Collection<KeyedWindowResult> result) {
        assertThat(result).containsExactlyInAnyOrder(
                new KeyedWindowResult("c1", START_TIME, 3));
    }

}
