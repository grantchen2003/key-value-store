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
    public synchronized long append(TransactionType txType, String key, String value) {
        final long offset = currentOffset.incrementAndGet();
        final Transaction transaction = new Transaction(offset, txType, key, value);
        queue.add(transaction);
        return offset;
    }

    @Override
    public synchronized List<Transaction> getTransactionsStartingFrom(long startOffset) {
        final List<Transaction> transactions = new ArrayList<>();
        for (final Transaction tx: queue) {
            if (tx.offset() >= startOffset) {
                transactions.add(tx);
            }
        }
        return transactions;
    }
}
