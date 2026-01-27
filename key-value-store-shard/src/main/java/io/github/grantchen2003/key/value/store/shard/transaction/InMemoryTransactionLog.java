package io.github.grantchen2003.key.value.store.shard.transaction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryTransactionLog implements TransactionLog {
    private final Queue<Transaction> queue = new ArrayDeque<>();
    private final AtomicLong currentOffset = new AtomicLong(0);

    @Override
    public synchronized void appendPut(String key, String value) {
        final Transaction tx = new PutTransaction(currentOffset.incrementAndGet(), key, value);
        queue.add(tx);
    }

    @Override
    public synchronized void appendDelete(String key) {
        final Transaction tx = new DeleteTransaction(currentOffset.incrementAndGet(), key);
        queue.add(tx);
    }

    @Override
    public synchronized List<Transaction> getTransactionsStartingFrom(long startOffset) {
        final List<Transaction> transactions = new ArrayList<>();
        for (final Transaction tx: queue) {
            if (tx.offset >= startOffset) {
                transactions.add(tx.copy());
            }
        }
        return transactions;
    }

    @Override
    public long size() {
        return currentOffset.get();
    }
}
