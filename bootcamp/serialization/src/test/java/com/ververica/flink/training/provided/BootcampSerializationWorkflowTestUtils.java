package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampSerializationWorkflow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BootcampSerializationWorkflowTestUtils {

    public static void testWorkflow(BootcampSerializationWorkflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();

        LongestTransactionSink longestTransactionSink = new LongestTransactionSink();
        longestTransactionSink.reset();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);

        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .setLongestTransactionsSink(longestTransactionSink)
                .build();

        env.execute("BootcampSerializationJob");

        BootcampTestUtils.validateOneMinuteResults(oneMinuteSink.getSink());
        BootcampTestUtils.validateFiveMinuteResults(fiveMinuteSink.getSink());
        BootcampTestUtils.validateLongestTransactionResults(longestTransactionSink.getSink());
    }

    private static class OneMinuteSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

    private static class FiveMinuteSink extends MockSink<WindowAllResult> {

        private static ConcurrentLinkedQueue<WindowAllResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<WindowAllResult> getSink() {
            return QUEUE;
        }
    }

    private static class LongestTransactionSink extends MockSink<KeyedWindowResult> {

        private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
            return QUEUE;
        }
    }

}