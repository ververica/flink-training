package com.ververica.flink.training.common;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@DoNotChangeThis
public class ShoppingCartSource extends FakeParallelSource<ShoppingCartRecord> {


    // Create a bounded source with a short delay between each record
    public ShoppingCartSource(long numRecords) {
        super(numRecords, 10L, true, getShoppingCartGenerator());
    }
    
    public ShoppingCartSource(long numRecords, long delay) {
        super(numRecords, delay, true, getShoppingCartGenerator());
    }

    // Create an unbounded source that sends records as fast as possible.
    public ShoppingCartSource() {
        super(Long.MAX_VALUE, 0L, false, getShoppingCartGenerator());
    }

    private static SerializableFunction<Long, ShoppingCartRecord> getShoppingCartGenerator() {
        // Set starting time to be 1 day ago
        return new ShoppingCartGenerator(System.currentTimeMillis() - Duration.ofDays(1).toMillis());
    }

}
