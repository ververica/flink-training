package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Convert a ShoppingCartRecord into <Country, total price in USD equivalent> tuple
 * by calling the currency rate API, and summing US$ equivalents * quantity
 * for every cart item
 */
class CalcTotalUSDollarPriceWithCacheFunction extends CalcTotalUSDollarPriceFunction {

    // A cache of previously calculated exchange rates.
    private transient Map<String, Double> cachedRates;

    public CalcTotalUSDollarPriceWithCacheFunction(long startTime) {
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

        // TODO we need a cache key that represents the exchange rate for a given
        // country at a given point in time. We know rates are valid for some
        // amount of time, so use the CurrencyRateAPI::getRateTimeAsIndex() call
        // // for this part of the key.
        String cacheKey = "";

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
