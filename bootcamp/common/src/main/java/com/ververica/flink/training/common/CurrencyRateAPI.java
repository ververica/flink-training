package com.ververica.flink.training.common;

import com.ververica.flink.training.common.ExchangeRates;
import org.apache.flink.annotation.VisibleForTesting;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Given a starting seed and time, generate deterministic results
 * for future conversion rates from a given currency to USD, at a
 * point in time at/after the provided <startTime>
 */
@DoNotChangeThis
public class CurrencyRateAPI {

    /**
     * Map from country to historical exchange rates. Each entry in the rate is for a sequential one minute
     * window from the starting time to that window's start time.
     */
    private Map<String, List<Double>> exchangeRates;
    private long startTime;
    private Random rand;
    private AtomicInteger activeRequests;

    public CurrencyRateAPI() {
        this(666L, System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    }

    public CurrencyRateAPI(long startTime) {
        this(666L, startTime);
    }

    @VisibleForTesting
    public CurrencyRateAPI(long seed, long startTime) {
        this.startTime = startTime;
        this.rand = new Random(seed);
        this.activeRequests = new AtomicInteger(0);

        this.exchangeRates = new HashMap<>();
        for (String country : ExchangeRates.STARTING_RATES.keySet()) {
            List<Double> historicalRates = new ArrayList<>();
            historicalRates.add(ExchangeRates.STARTING_RATES.get(country));
            exchangeRates.put(country, historicalRates);
        }
    }

    public double getRate(String country, Duration time) {
        return getRate(country, time.toMillis());
    }

    /**
     * @param country Target country
     * @param time    Target time
     * @return exchange rate from country's currency to USD
     */
    public double getRate(String country, long time) {

        try {
            int numActive = activeRequests.incrementAndGet();
            sleepBasedOnActiveRequests(numActive);

            int rateIndex = getRateTimeAsIndex(time);

            if (country.equals("US")) {
                return 1.0;
            }

            List<Double> historicalRates = exchangeRates.get(country);
            if (historicalRates == null) {
                throw new IllegalArgumentException("Unknown country: " + country);
            }

            // Make sure nobody else modifies this while we're adding to it
            synchronized (historicalRates) {
                int numRatesAvailable = historicalRates.size();
                // Need to calc rates going forward to the target time
                for (int i = numRatesAvailable - 1; i < rateIndex; i++) {
                    double curRate = historicalRates.get(i);
                    double deltaRate = curRate * (rand.nextGaussian() / 10000);
                    double newRate = curRate + deltaRate;
                    historicalRates.add(newRate);
                }
            }

            return historicalRates.get(rateIndex);
        } catch (InterruptedException e) {
            // Just return a bogus value
            return 0.0;
        } finally {
            activeRequests.decrementAndGet();
        }
    }

    private void sleepBasedOnActiveRequests(int numActive) throws InterruptedException {
        double msDelayPerActive;
        if (numActive < 10) {
            msDelayPerActive = 0.1;
        } else if (numActive < 100) {
            msDelayPerActive = 1.0;
        } else {
            msDelayPerActive = 10.0;
        }

        double delayInMS = msDelayPerActive * numActive;
        long millisecondsOnly = (long)delayInMS;
        int nanosecondsOnly = (int)((delayInMS - millisecondsOnly) * 1_000_000);

        Thread.sleep(millisecondsOnly, nanosecondsOnly);
    }

    /**
     * @param time
     * @return Return an index into the list of rates, based on time and the
     * duration of a rate's validity. Currently that's one minute.
     */
    public int getRateTimeAsIndex(long time) {
        if (time < startTime) {
            throw new IllegalArgumentException("Time must be after start time");
        }

        long rateIndexAsLong = (time - startTime) / (60 * 1000L);
        if (rateIndexAsLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time is too far in the future");
        }

        return (int) rateIndexAsLong;
    }

}
