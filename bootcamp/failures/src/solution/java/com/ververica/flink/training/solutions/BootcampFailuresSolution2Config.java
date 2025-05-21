package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.exercises.BootcampFailuresConfig;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;

public class BootcampFailuresSolution2Config extends BootcampFailuresConfig {

    @Override
    public TransactionalMemorySink getResultsSink() {
        final boolean exactlyOnce = true;
        return new TransactionalMemorySink(exactlyOnce);
    }

    public StreamExecutionEnvironment getEnvironment() throws IOException, URISyntaxException {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{
                // reduce time between restarts
                "--restartdelay", "1"});

        Configuration config = new Configuration();

        // If we were using RocksDB for our state, and the size of our state was large, and we typically
        // didn't update most of the state during any given checkpoint interval, then turning on incremental
        // checkpointing would likely improve performance. But we're using heap map state, and we don't have
        // a lot of state, so this won't change anything.
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        // We can reduce latency by reducing the time that the network layer buffers results.
        config.set(ExecutionOptions.BUFFER_TIMEOUT, Duration.ofMillis(10));

        StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters, config,1);
        // Set up for exactly once mode, with a shorter checkpoint interval.
        final long checkpointInterval = 100L;
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);

        return env;
    }
}
