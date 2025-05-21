package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ShoppingCartRecord;
import com.ververica.flink.training.common.CurrencyRateAPI;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Convert a ShoppingCartRecord into <Country, total price in USD equivalent> tuple
 * by calling the currency rate API, and summing US$ equivalents * quantity
 * for every cart item
 */
class CalcTotalUSDollarPriceFunction extends RichMapFunction<ShoppingCartRecord, Tuple2<String, Double>> {

    private final long startTime;
    // This has to be transient, since it's not serializable.
    private transient CurrencyRateAPI api;

    public CalcTotalUSDollarPriceFunction(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Since the CurrencyRateAPI isn't serializable (like many external APIs),
        // we create it in the open call.
        // TODO - make it so.
    }

    protected double getRate(String country, long time) {
        return api.getRate(country, time);
    }

    protected int getRateTimeAsIndex(long time) {
        return api.getRateTimeAsIndex(time);
    }

    protected double calcUSDEquivalent(ShoppingCartRecord in, double rate) {
        // Given a shopping cart and an exchange rate, calc the total USD amount
        // by iterating over cart items, and calculating the USD price * quantity,
        // and summing that.
        double usdEquivalentTotal = 0.0;

        // TODO - make it so.

        return usdEquivalentTotal;
    }

    @Override
    public Tuple2<String, Double> map(ShoppingCartRecord in) throws Exception {
        // Use the calcUSDEquivalent() method to calculate the Tuple2<Country,
        // USD value> result that we return.
        // TODO - make it so.
        return Tuple2.of("", 0.0);
    }
}
