# Lab: Bootcamp Serialization

## Introduction

This workflow is exactly the same as the `BootcampWindowingWorkflow`, in that we're calculating
three different results (per-minute/per-country item count, per-five-minute item count, and
per five-minute longest transactions).

The goal of this lab is to improve throughput of the workflow, by modifying the 
[BootcampSerializationWorkflow](src/main/java/com/ververica/flink/training/exercises/BootcampSerializationWorkflow.java)
class to improve serialization performance. Before starting this work, and after each improvement,
get an estimate of the workflow's throughput via one of two methods:

1. Run the `BootcampSerializationJob::main` method. At the end, a total time will be printed. This
   result includes the startup time for the workflow, so it's very approximate.
2. Run the `BootcampSerializationJob::main` method, and then use the Flink WebUI to view the
   `0.numRecordsOutPerSecond` metric for the ShoppingCartRecord source operator (the left-most
   operator in the execution graph). View it as a chart, and wait for the graph to stabilize before
   mousing over the high point to get a value. It helps to display the chart in "Big" mode. You
   might need to change the `numRecords` value in the `main()` method to `0`, so that the workflow
   doesn't terminate before you get a stable result.
   ```java
    public static void main(String[] args) throws Exception {
        final boolean discarding = true; // We always want to discard, to avoid performance impact from printing.
        final int parallelism = 2;
        final long numRecords = 10_000_000; // Set to 0 for unbounded source
   ```

You can also use the flamegraph profiling results to help in figuring out
where Flink is spending most of its available CPU cycles.

## Exercise 1

For this first exercise, you can improve performance a few different ways:

- Create a new class (let's call it `TrimmedShoppingCart`) that has **only** the fields from the
  `ShoppingCartRecord` that are used by the workflow, and convert from the incoming
  `ShoppingCartRecord` to this `TrimmedShoppingCart` as soon as possible.
- Make sure this `TrimmedShoppingCart` is serializable as a POJO, and thus doesn't use the (slower)
  Kryo serializer. To test this, you can use Flink's `PojoTestUtils.assertSerializedAsPojoWithoutKryo()`
  method. Note that you'll need to use the `@TypeInfo` annotation with the (provided)
  `ListInfoFactory`.
- Modify the `ShoppingCartRecord` (in the common sub-project) to also use the `@TypeInfo` annotation.

To test that your changes haven't broken anything, run the
[BootcampSerializationWorkflowTest](src/test/java/com/ververica/flink/training/exercises/BootcampSerializationWorkflowTest.java)
in IntelliJ.

## Exercise 2

There are also inefficiencies caused by the approach being taken to find the longest durations.
These changes are harder...

- Use a simple structure for top two durations, versus a `PriorityQueue`. This won't change
  the throughput very much, but if you were trying to capture the top N transactions, and there
  were a lot of these, and you had frequent checkpoints, then it would become an issue.
- (very hard) Use a `KeyedProcessFunction` to find transaction durations, versus Flink's session window
  support. This is hard because you'll have to set timers yourself, and ensure that the event time
  for the records you generate is based on the end of the session, not the timer's time.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
