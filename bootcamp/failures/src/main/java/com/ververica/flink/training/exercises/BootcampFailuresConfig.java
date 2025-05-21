package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.FlinkClusterUtils;
import com.ververica.flink.training.provided.TransactionalMemorySink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;

public class BootcampFailuresConfig {

    public TransactionalMemorySink getResultsSink() {
        return new TransactionalMemorySink();
    }

    public StreamExecutionEnvironment getEnvironment() throws IOException, URISyntaxException {
        ParameterTool parameters = ParameterTool.fromArgs(new String[]{
                "--parallelism", "1",
                // reduce time between restarts
                "--restartdelay", "1"});
         return FlinkClusterUtils.createConfiguredLocalEnvironment(parameters);
    }
}
