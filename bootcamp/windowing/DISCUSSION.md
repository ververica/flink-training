# Lab: Bootcamp Windowing (Discussion)

We'll work through the solutions to the three exercises.

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

1. Filter out uncompleted transactions
   ```java
        DataStream<ShoppingCartRecord> filtered = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()))
                .filter(r -> r.isTransactionCompleted());
   ```
1. Key by the country, and window
   ```java
           filtered.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
   ```
1. Aggregate using a custom `AggregationFunction` and `ProcessWindowFunction`, and
   write the results to the sink.
   ```java
                .aggregate(new CountItemsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
   ```
   
The `CountItemsAggregator` function is pretty simple. It's mostly Aggregator boilerplate
code, with the only real custom bit being the `add()` method.

```java
private static class CountItemsAggregator implements AggregateFunction<ShoppingCartRecord, Long, Long> {
    @Override
    public Long createAccumulator() {  return 0L; }

    @Override
    public Long add(ShoppingCartRecord value, Long acc) {
        for (CartItem item : value.getItems()) {
            acc += item.getQuantity();
        }

        return acc;
    }

    @Override
    public Long getResult(Long acc) { return acc; }

    @Override
    public Long merge(Long a, Long b) { return a + b; }
}
```

Similarly, the `SetKeyAndTimeFunction` function is simple - all it has to do is
use the window context to generate a result that includes both the key and the
window time, along with the aggregator's result:
```java
    private static class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
            out.collect(new KeyedWindowResult(key, ctx.window().getStart(), elements.iterator().next()));
        }
    }
```

As a thought experiment, let's say you added support for capturing late data, versus dropping it
(which is what happens now). How would you also add a test to validate proper behavior?

## Exercise 2 Solution

See the [README](README.md#exercise-2) file for the steps.

1. Do the same as before to calculate the 1 minute window, but don't immediately
   send it to a sink. Instead, save it as a stream, which can be sent to the
   `oneMinuteSink`, and also used as the starting stream for calculating the
   five minute window.
   ```java
   DataStream<KeyedWindowResult> oneMinuteStream = filtered
       .keyBy(r -> r.getCountry())
       .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
       .aggregate(new CountCartItemsAggregator(), new SetKeyAndTimeFunction());

    oneMinuteStream
                .sinkTo(oneMinuteSink);
   ```
1. Now use that `oneMinuteStream` with a `.windowAll()`, which means it's an
   unkeyed aggregation (for all countries). Aggregate using a custom 
  `AggregationFunction` and `ProcessWindowFunction`, and
   write the results to the `fiveMinuteSink`. Note that the `oneMinuteStream`
   generates records with their event time set to the end of the window that
   contained the records used to generate the aggregation result, so that the
   downstream `.windowAll()` works as expected.
   ```java
        DataStream<WindowAllResult> fiveMinuteStream = oneMinuteStream
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(new OneMinuteWindowCountAggregator(), new SetTimeFunction());

        fiveMinuteStream
                .sinkTo(fiveMinuteSink);
   ```

The `OneMinuteWindowCountAggregator` function is a simple aggregator.
```java
    private static class OneMinuteWindowCountAggregator implements AggregateFunction<KeyedWindowResult, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(KeyedWindowResult value, Long acc) {
            return acc + value.getResult();
        }

        @Override
        public Long getResult(Long acc) { return acc; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }
```

The `SetTimeFunction` is essentially the same as with the previous solution:
```java
    private static class SetTimeFunction extends ProcessAllWindowFunction<Long, WindowAllResult, TimeWindow> {

        @Override
        public void process(Context ctx, Iterable<Long> elements, Collector<WindowAllResult> out) throws Exception {
            out.collect(new WindowAllResult(ctx.window().getStart(), elements.iterator().next()));
        }
    }

```

Note that you could have started the five-minute aggregation from the `filtered` stream. But this
would be less efficient, since you'd be aggregating more records, versus just the 25 (one per test
country per minute * 5 minutes) records in the `oneMinuteStream`. When a record is emitted from
a `ProcessWindowFunction`, the timestamp is automatically set to the end of the window period.

## Exercise 3 Solution

See the [README](README.md#exercise-3) file for the steps.

In order to calculate the duration of a "session", we have to first group by the
`ShoppingCartRecord::transactionId`, and then find the window that contains all
the records, from first to last. But we can't use a fixed-size window, as a
user could be repeatedly adding/removing items for a very long time. We could
compensate for this by using a very large window, but that would impact the
latency of our results. One solution is to use Flink's `EventTimeSessionWindows` 
support for sessions, though this does generate many, many windows that can
impact performance and significantly increase state size.

Once we have all transactions grouped into a session, we can calculate the
duration. This resulting stream can then be used to find the top 2 longest
transactions per our configurable time window.

1. Since we need to analyze **all** transactions (not just completed ones)
   to find sessions, don't filter the incoming `cartStream` immediately.
   We still need watermarks, so create an intermediate `watermarkedStream`.
   ```java
           DataStream<ShoppingCartRecord> watermarkedStream = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));
   ```
1. Use the `watermarkedStream` for the one minute and five minute results.
   ```java
        // Filter out pending carts, key by country, tumbling window per minute
        DataStream<KeyedWindowResult> oneMinuteStream = watermarkedStream
                .filter(r -> r.isTransactionCompleted())
        // ... and so on, same as before
   ```
1. Also use the `watermarkedStream` to find sessions.
   ```java
           watermarkedStream
                // Key by transaction id, window by transaction (session) and calculate duration.
                // Generate result as KeyedWindowResult(transaction id, time, duration)
                .keyBy(r -> r.getTransactionId())
                .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(1)))
                .aggregate(new FindTransactionBoundsFunction(), new SetDurationAndTimeFunction())
   ```
1. Then use the `KeyedWindowResult` (transactionId, window time, millisecond duration)
   to find the longest two transactions for any given (configuration size) window.
   ```java
                   // Window by configurable window size, aggregate using PriorityQueue
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(transactionWindowInMinutes)))
                .aggregate(new FindLongestTransactions(), new EmitLongestTransactions())
                .sinkTo(longestTransactionsSink);

   ```

The `FindLongestTransactions` function uses a `PriorityQueue` to keep track of the
two longest transactions. For just top 2 this isn't really necessary, but it does
make it easier to extend to handle top N. Note though that this can lead to performance
issues, as the `PriorityQueue` will have to be serialized using `Kryo` when saving
state.
```java
    private static class FindLongestTransactions implements AggregateFunction<KeyedWindowResult,
            PriorityQueue<KeyedWindowResult>, List<KeyedWindowResult>> {
        @Override
        public PriorityQueue<KeyedWindowResult> createAccumulator() {
            return new PriorityQueue<>(new TransactionDurationComparator());
        }

        @Override
        public PriorityQueue<KeyedWindowResult> add(KeyedWindowResult value, PriorityQueue<KeyedWindowResult> acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public List<KeyedWindowResult> getResult(PriorityQueue<KeyedWindowResult> acc) {
            List<KeyedWindowResult> result = new ArrayList<>();
            int numToReturn = Math.min(acc.size(), 2);
            for (int i = 0; i < numToReturn; i++) {
                result.add(acc.remove());
            }

            return result;
        }

        @Override
        public PriorityQueue<KeyedWindowResult> merge(PriorityQueue<KeyedWindowResult> a, PriorityQueue<KeyedWindowResult> b) {
            a.addAll(b);
            return a;
        }
    }

private static class TransactionDurationComparator
        implements Comparator<KeyedWindowResult> {
    @Override
    public int compare(KeyedWindowResult o1, KeyedWindowResult o2) {
        // Return inverse sort order, longest first
        return Long.compare(o2.getResult(), o1.getResult());
    }
}
```

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
