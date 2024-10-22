
# Lab: eCommerce Failures & Exactly-once

## Introduction

This lab is about how to properly configure your Flink execution environment and the
workflow's sources & sinks to generate correct results even if the workflow fails.

## Exercise 1

Run the `testGettingCorrectResultsAfterFailure` test in the 
[BootcampFailures1WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampFailuresWorkflowTest.java).
It should fail, due to duplicate results being generated as a result of the
workflow being restarted after a failure.

> Before starting on fixing the failure, record the latency results printed
> at the end of the failed test run. It should look something like:
> ```bash
> Min: 345, Max: 345, Average: 345
> ```

Modify the [BootcampFailuresConfig](src/main/java/com/ververica/flink/training/exercises/BootcampFailuresConfig.java)
file to return an execution environment that's been configured for exactly-once.
In order for the test to work reliably, your checkpoint interval should be one
second or less (but not too short, of course).

You'll also need to set up the configuration to return a sink which is configured
for exactly once.

## Exercise 2

After completing Exercise 1, run the `testLatency` test in the
[BootcampFailures1WorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampFailuresWorkflowTest.java).
It should succeed, and print out latency results that look something like:

```bash
Min: 1210, Max: 1210, Average: 1210
```

Take a minute to think about why latency is now so much higher. Then figure
out ways to reduce the latency, by modifying your `StreamExecutionEnvironment`
that's being returned by the `BootcampFailuresConfig` class.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
