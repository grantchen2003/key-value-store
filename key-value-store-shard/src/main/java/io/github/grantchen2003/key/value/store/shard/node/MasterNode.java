package io.github.grantchen2003.key.value.store.shard.node;

import io.github.grantchen2003.key.value.store.shard.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.TransactionLogHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.SlaveHandler;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MasterNode extends Node {
    private final TransactionLog transactionLog;
    private final Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();

    public MasterNode(int port, Store store, TransactionLog transactionLog) throws IOException {
        super(port, store);
        server.createContext("/slave", new LoggingHandler(new SlaveHandler(this)));
        server.createContext("/transaction-log", new LoggingHandler(new TransactionLogHandler(this)));
        this.transactionLog = transactionLog;
    }

    public void addSlaveAddress(InetSocketAddress slaveAddress) {
        slaveAddresses.add(slaveAddress);
    }

    public List<Transaction> getTransactionsStartingFrom(long startOffset) {
        return transactionLog.getTransactionsStartingFrom(startOffset);
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

    @Override
    protected void onStart() {
        System.out.println("Master node started.");
    }

    private void replicatePut(InetSocketAddress slaveAddr, String key, String value) {
        System.out.println("Putting to replica " + slaveAddr);
    }

    private void replicateRemove(InetSocketAddress slaveAddr, String key) {
        System.out.println("Removing from replica " + slaveAddr);
    }
}
