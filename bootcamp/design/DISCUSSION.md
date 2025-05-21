# Lab: Bootcamp Windowing (Discussion)

We'll work through the solution to the exercise.

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

We've taken the `BootcampDesignMamboWorkflow` and split it into two separate
workflows, the `BootcampDesignAnalyticsSolutionWorkflow` and the
`BootcampDesignDetectionSolutionWorkflow`. 

Since all the custom functions have been pulled out as separate provided classes,
the actual workflow is pretty simple. For example, here's the detection workflow.

```java
    public void build() {
        Preconditions.checkNotNull(abandonedStream, "abandonedStream must be set");
        Preconditions.checkNotNull(badCustomerSink, "badCustomerSink must be set");

        // Key by customer, tumbling window per minute.
        // Count number of unique products that were abandoned, then filter to
        // only the customers with too many abandoned products.
        final long abandonedLimit = maxAbandonedPerMinute;
        abandonedStream
                .keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountProductsAggregator(), new SetKeyAndTimeFunction())
                .filter(r -> r.getResult() >= abandonedLimit)
                .sinkTo(badCustomerSink);
    }
```

There's really only one tricky
bit of code, found in the [FilterCompletedTransactions](src/provided/java/com/ververica/flink/training/provided/FilterCompletedTransactions.java)
class, which has already been created for you. This is where we set the
event time for the generated result explicitly, versus relying on the time
set for us by Flink:

```java
        TimestampedCollector tsc = (TimestampedCollector) out;
        for (ShoppingCartRecord cart : in) {
            String customerId = cart.getCustomerId();
            long transactionTime = cart.getTransactionTime();
            for (CartItem item : cart.getItems()) {
                tsc.setAbsoluteTimestamp(transactionTime);
                tsc.collect(new AbandonedCartItem(transactionId, transactionTime, customerId, item.getProductId()));
            }
        }
```

As you can see, we cast the more generic `Collector` we've been given into the
`TimestampedCollector`, which has the `.setAbsoluteTimestamp()` method that
we can use to specify what timestamp will be used for the record that is then
passed to the `.collect()` method.

If we don't do this, the event time will be set to the end of the `Session`
window, which is equal to the latest event which was merged into this session
window, **plus** the maximum session window gap, minus 1 millisecond. And that's
not so useful to us. We have the cart transaction time, which is expected and
more concrete, so that's what we'll use.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
