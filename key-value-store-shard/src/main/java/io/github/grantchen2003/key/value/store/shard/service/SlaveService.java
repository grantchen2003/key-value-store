package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.replication.SlaveRegistrar;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveSyncer;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.DeleteTransaction;
import io.github.grantchen2003.key.value.store.shard.transaction.PutTransaction;

import java.util.Optional;

public class SlaveService {
    private final SlaveSyncer slaveSyncer;
    private final SlaveRegistrar slaveRegistrar;
    private final Store store;

    public SlaveService(Store store, SlaveRegistrar slaveRegistrar, SlaveSyncer slaveSyncer) {
        this.store = store;
        this.slaveSyncer = slaveSyncer;
        this.slaveRegistrar = slaveRegistrar;
    }

    public Optional<String> get(String key) {
        return store.getValue(key);
    }

    public void put(long txOffset, String key, String value) {
        final PutTransaction tx = new PutTransaction(txOffset, key, value);
        slaveSyncer.applyPutTransaction(tx);
    }

    public void delete(long txOffset, String key) {
        final DeleteTransaction tx = new DeleteTransaction(txOffset, key);
        slaveSyncer.applyDeleteTransaction(tx);
    }

    public void start() {
        System.out.println("Registering with master...");
        slaveRegistrar.register();
        System.out.println("Successfully registered with master.");

        System.out.println("Syncing with master...");
        slaveSyncer.syncWithMaster();
        System.out.println("Successfully synced with master.");
    }
}
