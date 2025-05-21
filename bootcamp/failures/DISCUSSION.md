# Lab: Bootcamp Failures (Discussion)

We'll work through the solutions to the two exercises.

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

1. Enable exactly-once checkpoints for the execution environment returned by
   your `BootcampFailuresConfig.getEnvironment()` method.
   ```java
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
   ```
1. Configure the results sink to run in exactly-once mode (with transactions)
   ```java
    public TransactionalMemorySink getResultsSink() {
        final boolean exactlyOnce = true;
        return new TransactionalMemorySink(exactlyOnce);
    }
   ```

## Exercise 2 Solutions

See the [README](README.md#exercise-2) file for the steps.

1. Reduce the checkpoint interval. You can't make this too low, otherwise you'll
   start getting failed checkpoints due to the time spent creating the checkpoint
   exceeding this interval.
   ```java
   StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredLocalEnvironment(parameters, 1);
   // Set up for exactly once mode, with a shorter checkpoint interval.
   final long checkpointInterval = 100L;
   env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
   ```
   Changing this to something like 100ms dramatically reduces the latency:
   ```bash
   Min: 313, Max: 313, Average: 313
   ```
1. If we were using RocksDB, and we had a lot of state, then it could help to
   enable incremental checkpoints. But since we're using the Heap Map state
   backend, this won't change anything (it's ignored currently).
   ```java
   config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
   ```
1. You can also reduce latency by reducing the amount of time before the
   network layer flushes records from a buffer that has not been filled.
   ```java
   config.set(ExecutionOptions.BUFFER_TIMEOUT, Duration.ofMillis(10));
   ```
   This can reduce time by roughly the delta between 100ms and what you set,
   times the number of network shuffles in your workflow execution graph.
   But you don't want to set it too low, as that can reduce throughput.
   Note that in our exercise, we're running with a parallelism of 1, so
   there's no true network shuffle, though records are buffered before
   being forwarded locally.
   ```bash
   Min: 308, Max: 308, Average: 308
   ```

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
