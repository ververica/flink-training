package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.MockSink;

import java.util.concurrent.ConcurrentLinkedQueue;

public class BadCustomerSink extends MockSink<KeyedWindowResult> {

    private static ConcurrentLinkedQueue<KeyedWindowResult> QUEUE = new ConcurrentLinkedQueue<>();

    @Override
    public ConcurrentLinkedQueue<KeyedWindowResult> getSink() {
        return QUEUE;
    }
}
