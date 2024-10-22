package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.exercises.BootcampFailuresConfig;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;

public class BootcampFailuresSolution1Config extends BootcampFailuresConfig {

    @Override
    public TransactionalMemorySink getResultsSink() {
        final boolean exactlyOnce = true;
        return new TransactionalMemorySink(exactlyOnce);
    }

    public StreamExecutionEnvironment getEnvironment() throws IOException, URISyntaxException {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{
                "--parallelism", "1",
                // reduce time between restarts
                "--restartdelay", "1"});
        StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters);
        // Set up for exactly once mode.
        env.enableCheckpointing(Duration.ofSeconds(1).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
