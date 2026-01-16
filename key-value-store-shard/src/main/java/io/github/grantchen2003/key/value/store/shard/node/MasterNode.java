package io.github.grantchen2003.key.value.store.shard.node;

import io.github.grantchen2003.key.value.store.shard.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.SlaveHandler;
import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MasterNode extends Node {
    private final Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();

    public MasterNode(int port, Store store) throws IOException {
        super(port, store);
        server.createContext("/slave", new LoggingHandler(new SlaveHandler(this)));
    }

    public void addSlaveAddress(InetSocketAddress slaveAddress) {
        slaveAddresses.add(slaveAddress);
    }

    @Override
    public void put(String key, String value) {
        store.put(key, value);

        for (final InetSocketAddress slaveAddress : slaveAddresses) {
            replicatePut(slaveAddress, key, value);
        }
    }

    @Override
    public Optional<String> remove(String key) {
        Optional<String> valueOpt = store.remove(key);

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
