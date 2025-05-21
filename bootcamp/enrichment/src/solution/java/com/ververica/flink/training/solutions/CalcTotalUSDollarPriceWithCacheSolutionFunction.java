package com.ververica.flink.training.solutions;

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
