package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.exercises.BootcampFailuresConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampFailuresWorkflowTestUtils {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;
    private static final long NUM_RECORDS = 1_000;

    /**
     * Common test code used by both exercises & solutions tests. The solutions provide
     * a different config object, one based on a class that extends BootcampFailuresConfig.
     *
     *
     * @param config - configuration for the workflow.
     * @param triggerFailure - if true, cause the workflow to fail the first time it's run.
     * @throws Exception
     */
    public static void testWorkflow(BootcampFailuresConfig config, boolean triggerFailure) throws Exception {
        final StreamExecutionEnvironment env = config.getEnvironment();

        final boolean unbounded = true;
        DataStream<ShoppingCartRecord> cartStream = env.fromSource(ShoppingCartFiles.makeCartFilesSource(unbounded),
                        WatermarkStrategy.noWatermarks(),
                        "Shopping Cart Text Stream")
                .map(s -> ShoppingCartRecord.fromString(s))
                .name("Shopping Cart Stream")
                .setParallelism(1);

        TransactionalMemorySink resultsSink = config.getResultsSink();

        new BootcampFailuresWorkflow()
                .setCartStream(cartStream)
                .setResultSink(resultsSink)
                .setTriggerFailure(triggerFailure)
                .build();

        JobClient client = env.executeAsync("BootcampFailuresWorkflowTest");
        if (triggerFailure) {
            // Wait for the job to restart, due to our one-time failure.
            while (!client.getJobStatus().isDone() && (client.getJobStatus().get() != JobStatus.RESTARTING)) {
                Thread.sleep(10L);
            }
        }

        // Wait for the job to be running normally.
        while (!client.getJobStatus().isDone() && (client.getJobStatus().get() != JobStatus.RUNNING)) {
            Thread.sleep(1L);
        }

        long startTime = System.currentTimeMillis();

        // Wait until at least N checkpoints have happened (actually n-1), so that we know the sink
        // has had a commit call. We'll also bail out after 5 seconds, which could happen if checkpointing
        // isn't happening, or something has stalled out.
        final int numCheckpoints = 3;
        File checkpointDir = new File(new URI(env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY)));
        long endTime = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
        while (!findCheckpointDir(checkpointDir, numCheckpoints) && (System.currentTimeMillis() < endTime)) {
            Thread.sleep(200L);
        }

        // Stop the workflow if it's still running.
        if (!client.getJobStatus().isDone()) {
            client.cancel();
        }

        // Calculate min/max/average latency. This is approximate, since our start time is loosely based
        // on when the job switched to the RUNNING state.
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long count = 0;
        long sum = 0;
        for (Tuple2<Long, String> record : resultsSink.getCommitted()) {
            long latency = record.f0 - startTime;
            min = Math.min(min, latency);
            max = Math.max(max, latency);
            sum += latency;
            count++;
        }

        assertThat(count).isGreaterThan(0);
        System.out.format("Min: %d, Max: %d, Average: %d\n", min, max, sum / count);

        List<String> records = new ArrayList<>();
        resultsSink.getCommitted().forEach(r -> records.add(r.f1));
        assertThat(records).containsExactlyInAnyOrder(
                new KeyedWindowResult("CA", START_TIME, 80L).toString(),
                new KeyedWindowResult("CN", START_TIME, 174L).toString(),
                new KeyedWindowResult("CN", START_TIME + Duration.ofMinutes(1).toMillis(), 14L).toString(),
                new KeyedWindowResult("JP", START_TIME, 8L).toString(),
                new KeyedWindowResult("JP", START_TIME + Duration.ofMinutes(1).toMillis(), 8L).toString(),
                new KeyedWindowResult("MX", START_TIME, 234L).toString(),
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1).toMillis(), 25L).toString(),
                new KeyedWindowResult("US", START_TIME, 773L).toString(),
                new KeyedWindowResult("US", START_TIME + Duration.ofMinutes(1).toMillis(), 104L).toString(),

                // We have a special 0 count entry that triggers the one-time exception.
                new KeyedWindowResult("MX", START_TIME + Duration.ofMinutes(1025).toMillis(), 0L).toString()
        );


    }

    /**
     * Utility routine that returns true if a directory exists inside of <checkpointDir>
     * for the target <checkpointNumber>
     *
     * @param checkpointDir
     * @param checkpointNumber
     * @return true if the checkpoint directory exists.
     */
    private static boolean findCheckpointDir(File checkpointDir, int checkpointNumber) {
        String targetDirName = String.format("chk-%d", checkpointNumber);
        LinkedList<File> stack = new LinkedList<>();
        stack.push(checkpointDir);

        while (!stack.isEmpty()) {
            File curDirectory = stack.pop();
            for (File f : curDirectory.listFiles()) {
                if (f.isDirectory()) {
                    if (f.getName().equals(targetDirName)) {
                        return true;
                    }

                    stack.push(f);
                }
            }
        }

        return false;
    }
}