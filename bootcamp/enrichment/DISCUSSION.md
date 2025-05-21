# Lab: Bootcamp Enrichment (Discussion)

We'll work through the solutions to the three exercises.

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

All that's actually needed is to make some fairly simple changes to the existing
`CalcTotalUSDollarPriceSolutionFunction` map function, to instantiate the API
used to get exchange rates in its `open()` method, and then use the API in the
`map()` method.

```java
    public void open(OpenContext openContext) throws Exception {
        // Since the CurrencyRateAPI isn't serializable (like many external APIs),
        // we create it in the open call.
        api = new CurrencyRateAPI(startTime);
    }
```

Note that this map function needs to `extend RichMapFunction`, so that there's an
`open()` method which it overrides to create the API when the Task Manager is
starting up this instance of the operator, after deserializing it from the job
jar that it was sent by the Job Manager.

```java
    protected double calcUSDEquivalent(ShoppingCartRecord in, double rate) {
        double usdEquivalentTotal = 0.0;

        for (CartItem item : in.getItems()) {
            double usdPrice = rate * item.getPrice();
            usdEquivalentTotal += (usdPrice * item.getQuantity());
        }

        return usdEquivalentTotal;
    }
```

## Exercise 2 Solution

See the [README](README.md#exercise-2) file for the steps.

This is much more challenging, as you have to first "explode" the shopping cart
into individual records (one per cart item) so that you can join this stream
against the (provided) product info stream.

```java
        // Turn into a per-product stream
        DataStream<ProductRecord> productStream = filtered
                // Use a flatMap to convert one shopping cart into 1...N ProductRecords.
                .flatMap(new ExplodeShoppingCartFunction())
                .name("Explode shopping cart");
```

Then we can join with the product info stream, and generate enriched results.

```java
        // Connect products with the product info stream, using product ID,
        // and enrich the product records. Both streams need to be keyed by the
        // product id before connecting. Then .process(custom KeyedCoProcessFunction())
        // can be used
        DataStream<ProductRecord> enrichedStream = productStream
                .keyBy(r -> r.getProductId())
                .connect(watermarkedProduct.keyBy(r -> r.getProductId()))
                // Use a KeyedCoProcessFunction to join the two streams of data.
                .process(new AddProductInfoFunction())
                .name("Enriched products");
```

Now that our stream of `ProductRecord` records contain the weight of each product,
we can calculate a sum of the weight.

```java
        // Key by country, tumbling window per minute
        enrichedStream.keyBy(r -> r.getCountry())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SumWeightAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(resultSink);
```

The `AddProductInfoFunction` is the most complicated piece of this solution. It uses
a very common "stateful join" pattern, where the fact records (records to be enriched)
are saved in ListState if they arrive before the enrichment record. And once the
enrichment record arrives, any such saved records can be enriched & emitted.

```java
public class AddProductInfoSolutionFunction extends KeyedCoProcessFunction<String, ProductRecord, ProductInfoRecord, ProductRecord> {

    private transient ListState<ProductRecord> pendingLeft;
    private transient ValueState<ProductInfoRecord> pendingRight;

    @Override
    public void open(OpenContext ctx) throws Exception {
        pendingLeft = getRuntimeContext().getListState(new ListStateDescriptor<>("left", ProductRecord.class));
        pendingRight = getRuntimeContext().getState(new ValueStateDescriptor<>("right", ProductInfoRecord.class));
    }

    @Override
    public void processElement1(ProductRecord left, Context ctx, Collector<ProductRecord> out) throws Exception {
        ProductInfoRecord right = pendingRight.value();
        if (right != null) {
            left.setCategory(right.getCategory());
            left.setWeightKg(right.getWeightKg());
            left.setProductName(right.getProductName());
            out.collect(left);
        } else {
            // We need to wait for the product info record to arrive.
            // Note this wouldn't be safe to do if the state backend was HeapMapState, and
            // we had objectReuse enabled.
            pendingLeft.add(left);
        }
    }

    @Override
    public void processElement2(ProductInfoRecord right, Context ctx, Collector<ProductRecord> out) throws Exception {
        pendingRight.update(right);

        // If there are any pending left-side records, output the enriched version now.
        for (ProductRecord left : pendingLeft.get()) {
            left.setCategory(right.getCategory());
            left.setWeightKg(right.getWeightKg());
            left.setProductName(right.getProductName());
            out.collect(left);
        }

        pendingLeft.clear();
    }
```

## Exercise 3 Solution

See the [README](README.md#exercise-3) file for the steps.

This is a simple extension to the `CalcTotalUSDollarPriceFunction`, where we
add a cache to reduce the latency/throughput impact from calling an API for
every record, and we reduce the load on the API system.

The only somewhat-interesting bit is how we need to calculate a cache key
that includes the validity time for the rate information, so that we don't
use out-of-date information. This does, however, create the potential for
unbounded growth of the cache, which means we could add some limits on
the total size of the cache.

```java
class CalcTotalUSDollarPriceWithCacheSolutionFunction extends CalcTotalUSDollarPriceSolutionFunction {

    private transient Map<String, Double> cachedRates;

    public CalcTotalUSDollarPriceWithCacheSolutionFunction(long startTime) {
        super(startTime);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        cachedRates = new HashMap<>();
    }

    @Override
    public Tuple2<String, Double> map(ShoppingCartRecord in) throws Exception {
        String country = in.getCountry();
        long transactionTime = in.getTransactionTime();
        String cacheKey = String.format("%s-%d", country, getRateTimeAsIndex(transactionTime));

        double rate;
        if (cachedRates.containsKey(cacheKey)) {
            rate = cachedRates.get(cacheKey);
        } else {
            rate = getRate(country, transactionTime);
            cachedRates.put(cacheKey, rate);
        }

        return Tuple2.of(country, calcUSDEquivalent(in, rate));
    }
}
```

## More Optimizations

1. Are there more caching optimizations we could do in the `CalcTotalUSDollarPriceWithCacheSolutionFunction`?
1. What would happen if we called the `CalcTotalUSDollarPriceWithCacheSolutionFunction`
   map function after we did `.keyBy(country)`? Would that help or hurt our performance?
1. Assuming doing #1 above hurt, how could we adjust the workflow to mitigate
   the impact of key skew?
2. What would change if we used AsyncIO when calling the API?
3. What would need to change if the API only returned current exchange rates?
-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
