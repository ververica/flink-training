package com.ververica.flink.training.common;

import com.ververica.flink.training.common.CurrencyRateAPI;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CurrencyRateAPITest {

    @Test
    void testGetRateWithUS() {
        final long seed = 666L;
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        double rate = api.getRate("US", 0);
        assertEquals(1.0, rate);

        assertEquals(1.0, api.getRate("US", Duration.ofMinutes(1)));
    }

    @Test
    void testGetStableRates() {
        final long seed = 666L;
        final String country = "JP";
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        // Get rates filled in for 0...5 minutes
        double atFive = api.getRate(country, Duration.ofMinutes(5));

        // Get some target stable values
        double atZero = api.getRate(country, Duration.ofMinutes(0));
        double atThree = api.getRate(country, Duration.ofMinutes(3));

        // Verify they don't change with subsequent calls
        assertEquals(atZero, api.getRate(country, Duration.ofMinutes(0)));
        assertEquals(atThree, api.getRate(country, Duration.ofMinutes(3)));
        assertEquals(atFive, api.getRate(country, Duration.ofMinutes(5)));
    }

    @Test
    void testManyRates() {
        final long seed = 666L;
        final String country = "JP";
        CurrencyRateAPI api = new CurrencyRateAPI(seed, 0);

        api.getRate(country, Duration.ofDays(5));
    }

    @Test
    void testConcurrency() throws InterruptedException {
        final long seed = 666L;
        final String country = "JP";
        final CurrencyRateAPI regularApi = new CurrencyRateAPI(seed, 0);
        final int numTimeSlots = 20;
        final double[] rates = new double[numTimeSlots];
        for (int i = 0; i < numTimeSlots; i++) {
            rates[i] = regularApi.getRate(country, Duration.ofMinutes(i));
        }

        final CurrencyRateAPI concurrent = new CurrencyRateAPI(seed, 0);
        final AtomicBoolean start = new AtomicBoolean(false);
        final ThreadGroup tg = new ThreadGroup("Concurrency test");

        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(tg, new Runnable() {

                @Override
                public void run() {
                    while (!start.get()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    for (int j = 0; j < numTimeSlots; j++) {
                        assertEquals(rates[j], concurrent.getRate(country, Duration.ofMinutes(j)));
                    }
                }
            });

            t.start();
        }

        start.set(true);
        while (tg.activeCount() > 0) {
            Thread.sleep(1);
        }
    }

}