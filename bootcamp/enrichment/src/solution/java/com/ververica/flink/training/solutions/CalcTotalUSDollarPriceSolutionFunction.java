package com.ververica.flink.training.solutions;

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
class CalcTotalUSDollarPriceSolutionFunction extends RichMapFunction<ShoppingCartRecord, Tuple2<String, Double>> {

    private final long startTime;
    private transient CurrencyRateAPI api;

    public CalcTotalUSDollarPriceSolutionFunction(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Since the CurrencyRateAPI isn't serializable (like many external APIs),
        // we create it in the open call.
        api = new CurrencyRateAPI(startTime);
    }

    protected double getRate(String country, long time) {
        return api.getRate(country, time);
    }

    protected int getRateTimeAsIndex(long time) {
        return api.getRateTimeAsIndex(time);
    }

    protected double calcUSDEquivalent(ShoppingCartRecord in, double rate) {
        double usdEquivalentTotal = 0.0;

        for (CartItem item : in.getItems()) {
            double usdPrice = rate * item.getPrice();
            usdEquivalentTotal += (usdPrice * item.getQuantity());
        }

        return usdEquivalentTotal;
    }

    @Override
    public Tuple2<String, Double> map(ShoppingCartRecord in) throws Exception {
        String country = in.getCountry();
        double rate = getRate(country, in.getTransactionTime());
        return Tuple2.of(country, calcUSDEquivalent(in, rate));
    }
}
