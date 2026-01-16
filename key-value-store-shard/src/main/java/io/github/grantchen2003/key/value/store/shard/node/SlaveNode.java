package io.github.grantchen2003.key.value.store.shard.node;

import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

public class SlaveNode extends Node {
    private final InetSocketAddress masterAddress;

    public SlaveNode(int port, Store store, InetSocketAddress masterAddress) throws IOException {
        super(port, store);
        this.masterAddress = masterAddress;
    }

    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    @Override
    public Optional<String> remove(String key) {
        return store.remove(key);
    }

    @Override
    protected void onStart() {
        System.out.println("Slave node started");
        System.out.println("Registering with master...");
        registerWithMaster();
        System.out.println("Successfully registered with master...");
    }

    private void registerWithMaster() {
        System.out.println("Sending PUT request to master at " + masterAddress);
        // TODO: implement PUT request to /slave?address=...
    }
}
