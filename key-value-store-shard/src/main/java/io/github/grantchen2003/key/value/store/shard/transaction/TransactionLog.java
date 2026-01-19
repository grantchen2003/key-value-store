package io.github.grantchen2003.key.value.store.shard.transaction;

import java.util.List;

public interface TransactionLog {
    void append(TransactionType txType, String key, String value);
    List<Transaction> getTransactionsStartingFrom(long startOffset);
    void removeTransactionsUpToAndIncluding(long upToOffset);
    long getCurrentOffset();
}
