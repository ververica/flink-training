package com.ververica.flink.training.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyedWindowResultTest {

    @Test
    public void testRoundTrip() {
        KeyedWindowResult r1 = new KeyedWindowResult("key", System.currentTimeMillis(), 333L);
        String r1AsString = r1.toString();
        KeyedWindowResult r2 = KeyedWindowResult.fromString(r1AsString);
        assertEquals(r1, r2);
    }
}