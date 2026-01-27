package io.github.grantchen2003.key.value.store.shard.transaction;

import java.util.List;

public interface TransactionLog {
    void appendPut(String key, String value);
    void appendDelete(String key);
    List<Transaction> getTransactionsStartingFrom(long startOffset);
    long size();
}
