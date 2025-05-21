/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;

import static org.apache.flink.configuration.RestOptions.BIND_PORT;
import static org.apache.flink.configuration.RestOptions.ENABLE_FLAMEGRAPH;
import static org.apache.flink.configuration.TaskManagerOptions.*;

/** Common functionality to set up execution environments for the bootcamp training. */
@DoNotChangeThis
public class FlinkClusterUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkClusterUtils.class);

    public static final int NO_WEBUI_PORT = -1;

    public static StreamExecutionEnvironment createConfiguredTestEnvironment(int parallelism) throws IOException, URISyntaxException {
        return createConfiguredTestEnvironment(ParameterTool.fromArgs(new String[]{}), parallelism);
    }

    public static StreamExecutionEnvironment createConfiguredTestEnvironment(
            final ParameterTool parameters, int parallelism) throws IOException, URISyntaxException {
        return createEnvironment(parameters, new Configuration(), parallelism, true, NO_WEBUI_PORT);
    }

    public static StreamExecutionEnvironment createConfiguredLocalEnvironment(
            final ParameterTool parameters, int parallelism) throws IOException, URISyntaxException {
        return createEnvironment(parameters, new Configuration(), parallelism, true,
                Integer.parseInt(BIND_PORT.defaultValue()));
    }

    public static StreamExecutionEnvironment createConfiguredLocalEnvironment(
            final ParameterTool parameters) throws IOException, URISyntaxException {
        int parallelism = parameters.getInt("parallelism", -1);
        return createConfiguredLocalEnvironment(parameters, parallelism);
    }

    public static StreamExecutionEnvironment createConfiguredLocalEnvironment(
            final ParameterTool parameters, Configuration extraConfig) throws IOException, URISyntaxException {
        int parallelism = parameters.getInt("parallelism", -1);
        return createConfiguredLocalEnvironment(parameters, extraConfig, parallelism);
    }

    public static StreamExecutionEnvironment createConfiguredLocalEnvironment(
            final ParameterTool parameters, Configuration extraConfig, int parallelism) throws IOException, URISyntaxException {
        return createEnvironment(parameters, extraConfig, parallelism, true, Integer.parseInt(BIND_PORT.defaultValue()));
    }

    /**
     * Creates a streaming environment with a few pre-configured settings based on command-line
     * parameters.
     *
     * @throws IOException if the local checkpoint directory for the file system state backend*
     *     cannot be created
     * @throws URISyntaxException if <code>fsStatePath</code> is not a valid URI
     */
    public static StreamExecutionEnvironment createConfiguredEnvironment(
            final ParameterTool parameters) throws IOException, URISyntaxException {
        final boolean local = isLocal(parameters);
        final int webUIPort = local ? Integer.parseInt(BIND_PORT.defaultValue()) : NO_WEBUI_PORT;
        final int parallelism = parameters.getInt("parallelism", -1);
        return createEnvironment(parameters, new Configuration(), parallelism, local, webUIPort);
    }

    private static StreamExecutionEnvironment createEnvironment(
            final ParameterTool parameters, Configuration extraConfig,
            int parallelism, boolean local, int webUIPort) throws IOException, URISyntaxException {
        final StreamExecutionEnvironment env;
        if (!local) {
            // cluster mode or disabled web UI
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        } else {
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(CPU_CORES, 4.0);
            flinkConfig.set(TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
            flinkConfig.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
            flinkConfig.set(MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(1024));

            // Enable Flamegraphs
            flinkConfig.set(ENABLE_FLAMEGRAPH, true);

            // configure directory for JobManager log files
            Files.createTempDirectory("flink-logfiles");
            System.setProperty("log.file", Files.createTempDirectory("flink-logfiles").toString());

            // configure filesystem state backend
            String statePath = parameters.get("fsStatePath");
            Path checkpointPath;
            if (statePath != null) {
                FileUtils.deleteDirectory(new File(new URI(statePath)));
                checkpointPath = Path.fromLocalFile(new File(new URI(statePath)));
            } else {
                checkpointPath =
                        Path.fromLocalFile(Files.createTempDirectory("flink-checkpoints").toFile());
            }
            // Ensure that when a bounded source finishes, a final checkpoint happens, which will cause
            // any pending sink output to flush.
            flinkConfig.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

            if (parameters.has("useRocksDB")) {
                flinkConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            } else {
                flinkConfig.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            }

            LOG.info("Writing checkpoints to {}", checkpointPath);
            flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toString());

            // set a restart strategy for better IDE debugging
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
            Duration restartDelay = Duration.ofSeconds(parameters.getInt("restartdelay", 15));
            flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, restartDelay);

            // Mix in any provided configuration, which could override what we set above (other than the
            // webUI port, which is explicitly provided).
            flinkConfig.addAll(extraConfig);

            if (webUIPort != NO_WEBUI_PORT) {
                // configure Web UI
                flinkConfig.set(BIND_PORT, Integer.toString(webUIPort));
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
            } else {
                env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
            }
        }

        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }

        env.getConfig().setGlobalJobParameters(parameters);
        return env;
    }

    /** Checks whether the environment should be set up in local mode. */
    public static boolean isLocal(ParameterTool parameters) {
        final String localMode = parameters.get("local");
        if (localMode == null) {
            return System.getenv("FLINK_TRAINING_LOCAL") != null;
        } else {
            return !localMode.equals(Integer.toString(NO_WEBUI_PORT));
        }
    }
}
