package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.replication.ReplicationStreamer;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MasterService {
    private final Store store;
    private final TransactionLog transactionLog;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition txAvailable = lock.newCondition();

    public MasterService(Store store, TransactionLog transactionLog) {
        this.store = store;
        this.transactionLog = transactionLog;
    }

    public synchronized Optional<String> get(String key) {
        return store.getValue(key);
    }

    public void put(String key, String value) {
        lock.lock();
        try {
            transactionLog.appendPut(key, value);
            store.put(key, value);
            txAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Optional<String> delete(String key) {
        lock.lock();
        try {
            transactionLog.appendDelete(key);
            final Optional<String> result = store.remove(key);
            txAvailable.signalAll();
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void addSlave(InetSocketAddress slaveAddress) {
        System.out.println("MASTER: Registering new slave at " + slaveAddress);
        Thread.startVirtualThread(new ReplicationStreamer(
                slaveAddress,
                this.transactionLog,
                this.lock,
                this.txAvailable
        ));
    }

    public List<Transaction> getTransactionsStartingFrom(long startOffset) {
        return transactionLog.getTransactionsStartingFrom(startOffset);
    }
}