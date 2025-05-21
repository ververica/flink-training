package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BootcampWindowingWorkflowTestUtils {

    public static void testWindowing1Workflow(BootcampWindowing1Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        OneMinuteSink sink = new OneMinuteSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("BootcampWindowing1Workflow");

        BootcampTestUtils.validateOneMinuteResults(sink.getSink());
    }

    public static void testWindowing2Workflow(BootcampWindowing2Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();

        ParameterTool parameters = ParameterTool.fromArgs(new String[]{"--parallelism", "2"});
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredEnvironment(parameters);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .build();

        env.execute("BootcampWindowing2Workflow");

        BootcampTestUtils.validateOneMinuteResults(oneMinuteSink.getSink());
        BootcampTestUtils.validateFiveMinuteResults(fiveMinuteSink.getSink());
    }

    public static void testWindowing3Workflow(BootcampWindowing3Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        OneMinuteSink oneMinuteSink = new OneMinuteSink();
        FiveMinuteSink fiveMinuteSink = new FiveMinuteSink();
        LongestTransactionSink longestTransactionSink = new LongestTransactionSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setOneMinuteSink(oneMinuteSink)
                .setFiveMinuteSink(fiveMinuteSink)
                .setLongestTransactionsSink(longestTransactionSink)
                .build();

        env.execute("BootcampWindowing3Job");

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