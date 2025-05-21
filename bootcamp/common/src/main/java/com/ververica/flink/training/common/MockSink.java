package com.ververica.flink.training.common;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Base class for test sinks that save data to a thread-safe queue. This is based on the new Flink Sink interface, but
 * we don't support checkpoints, so all state is null/Void.
 *
 * @param <T>
 */
@SuppressWarnings("serial")
@DoNotChangeThis
public abstract class MockSink<T> implements Sink<T> {

    // Every extending class must implement this one method.
    public abstract ConcurrentLinkedQueue<T> getSink();

    public MockSink() {
        reset();
    }

    public MockSink<T> reset() {
        getSink().clear();
        return this;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext ctx) throws IOException {
        return new SinkWriter<T>() {

            @Override
            public void close() throws Exception {
            }

            @Override
            public void flush(boolean arg0) throws IOException, InterruptedException {
            }

            @Override
            public void write(T element, Context ctx) throws IOException, InterruptedException {
                getSink().add(element);
            }
        };
    }
}
