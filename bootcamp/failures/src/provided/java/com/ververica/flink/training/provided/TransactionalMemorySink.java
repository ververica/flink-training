package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 This implementation of a Flink sink maintains exactly-once semantics by:

 - Storing write operations in transactions.
 - Snapshotting the state of all transactions, including the current one.
 - Restoring the state of all transactions when needed.
 - Committing data during flush operations.
 */
@DoNotChangeThis
public class TransactionalMemorySink implements StatefulSink<String, List<String>> {

    // This is a static class member, because it represents the committed transactional
    // data (e.g. a DB's committed set of records).
    private static final ConcurrentLinkedQueue<Tuple2<Long, String>> COMMITTED = new ConcurrentLinkedQueue<>();

    // This is a static class member, because it represents the sink persistent global state
    // (e.g. a DB's set of transactions), so we don't want it cleared when the workflow is
    // re-started after a failure.
    private static final ConcurrentHashMap<Long, List<String>> TRANSACTIONS = new ConcurrentHashMap<>();

    private boolean exactlyOnce;

    public TransactionalMemorySink() {
        this(false);
    }

    public TransactionalMemorySink(boolean exactlyOnce) {
        this.exactlyOnce = exactlyOnce;

        reset();
    }

    @Override
    public StatefulSinkWriter<String, List<String>> createWriter(InitContext context) throws IOException {
        return new MemorySinkWriter(context.getSubtaskId(), exactlyOnce);
    }

    @Override
    public StatefulSinkWriter<String, List<String>> restoreWriter(
            WriterInitContext context, Collection<List<String>> recoveredState) throws IOException {
        int subtaskId = new InitContextWrapper(context).getSubtaskId();
        ArrayList<String> transactions = new ArrayList<>();
        recoveredState.forEach(t -> transactions.addAll(t));
        return new MemorySinkWriter(subtaskId, exactlyOnce, transactions);
    }

    @Override
    public SimpleVersionedSerializer<List<String>> getWriterStateSerializer() {
        return new StringListSerializer();
    }

    private class MemorySinkWriter implements StatefulSinkWriter<String, List<String>> {

        private final int subtaskId;
        private long transactionId = 0;

        private boolean exactlyOnce;

        private List<String> currentTransaction;

        public MemorySinkWriter(int subtaskId, boolean exactlyOnce) {
            this(subtaskId, exactlyOnce, new ArrayList<>());
        }

        public MemorySinkWriter(int subtaskId, boolean exactlyOnce, List<String> currentTransaction) {
            this.subtaskId = subtaskId;
            this.exactlyOnce = exactlyOnce;
            this.currentTransaction = currentTransaction;
        }

        @Override
        public void write(String element, Context context) throws IOException, InterruptedException {
            if (exactlyOnce) {
                currentTransaction.add(element);
            } else {
                COMMITTED.add(Tuple2.of(System.currentTimeMillis(), element));
            }
        }

        @Override
        public List<List<String>> snapshotState(long checkpointId) throws IOException {
            List<List<String>> state = new ArrayList<>(TRANSACTIONS.values());
            state.add(new ArrayList<>(currentTransaction));
            return state;
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (endOfInput || !currentTransaction.isEmpty()) {
                if (exactlyOnce) {
                    TRANSACTIONS.put(transactionId++, new ArrayList<>(currentTransaction));
                    currentTransaction.forEach(r -> COMMITTED.add(Tuple2.of(System.currentTimeMillis(), r)));
                }

                currentTransaction.clear();
            }
        }

        @Override
        public void close() throws Exception {
            // No-op for in-memory sink
        }
    }

    private static class StringListSerializer implements SimpleVersionedSerializer<List<String>> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(List<String> state) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(state);
                return bos.toByteArray();
            }
        }

        @Override
        public List<String> deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 ObjectInputStream ois = new ObjectInputStream(bis)) {
                return (List<String>) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize state", e);
            }
        }
    }

    public ConcurrentLinkedQueue<Tuple2<Long, String>> getCommitted() {
        return COMMITTED;
    }

    /**
     * Clear out any persisted state, in case we run multiple tests using this sink.
     *
     * @return
     */
    public TransactionalMemorySink reset() {
        COMMITTED.clear();
        TRANSACTIONS.clear();

        return this;
    }
}