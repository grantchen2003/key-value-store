package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.replication.write.replicator.AsyncWriteReplicator;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

public class MasterService {
    private final Store store;
    private final TransactionLog transactionLog;
    private final AsyncWriteReplicator asyncWriteReplicator;

    public MasterService(Store store, TransactionLog transactionLog, AsyncWriteReplicator asyncWriteReplicator) {
        this.store = store;
        this.transactionLog = transactionLog;
        this.asyncWriteReplicator = asyncWriteReplicator;
    }

    public synchronized Optional<String> get(String key) {
        return store.getValue(key);
    }

    public synchronized void put(String key, String value) {
        final long txOffset = transactionLog.appendPut(key, value);
        store.put(key, value);
        asyncWriteReplicator.replicatePutAsync(txOffset, key, value);
    }

    public synchronized Optional<String> remove(String key) {
        final long txOffset = transactionLog.appendDelete(key);
        final Optional<String> valueOpt = store.remove(key);
        asyncWriteReplicator.replicateRemoveAsync(txOffset, key);
        return valueOpt;
    }

    public void addSlaveAddress(InetSocketAddress slaveAddress) {
        asyncWriteReplicator.addSlave(slaveAddress);
    }

    public List<Transaction> getTransactionsStartingFrom(long startOffset) {
        return transactionLog.getTransactionsStartingFrom(startOffset);
    }
}
