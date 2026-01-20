package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionType;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MasterService extends Service {
    final TransactionLog transactionLog;
    private final Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();

    public MasterService(Store store, TransactionLog transactionLog) {
        super(store);
        this.transactionLog = transactionLog;
    }

    @Override
    public void put(String key, String value) {
        transactionLog.append(TransactionType.PUT, key, value);

        store.put(key, value);

        for (final InetSocketAddress slaveAddress : slaveAddresses) {
            replicatePut(slaveAddress, key, value);
        }
    }

    @Override
    public Optional<String> remove(String key) {
        transactionLog.append(TransactionType.DELETE, key, null);

        Optional<String> valueOpt = store.remove(key);

        // TODO: make this concurrent, learn more about java concurrency first
        for (final InetSocketAddress slaveAddress : slaveAddresses) {
            replicateRemove(slaveAddress, key);
        }

        return valueOpt;
    }


    public void addSlaveAddress(InetSocketAddress slaveAddress) {
        slaveAddresses.add(slaveAddress);
    }

    public List<Transaction> getTransactionsStartingFrom(long startOffset) {
        return transactionLog.getTransactionsStartingFrom(startOffset);
    }

    private void replicatePut(InetSocketAddress slaveAddress, String key, String value) {
        System.out.println("Putting to replica " + slaveAddress);
    }

    private void replicateRemove(InetSocketAddress slaveAddress, String key) {
        System.out.println("Removing from replica " + slaveAddress);
    }
}
