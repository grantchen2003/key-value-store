package io.github.grantchen2003.key.value.store.shard.server;

import io.github.grantchen2003.key.value.store.shard.config.ShardConfig;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveRegistrar;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveSyncer;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;
import io.github.grantchen2003.key.value.store.shard.store.InMemoryStore;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.InMemoryTransactionLog;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;

public class ServerFactory {
    private ServerFactory() {}

    public static Server createServer(int port, ShardConfig config) throws IOException {
        return switch (config.role()) {
            case MASTER -> createMasterServer(port);
            case SLAVE  -> createSlaveServer(port, config);
        };
    }

    private static Server createMasterServer(int port) throws IOException {
        final Store store = new InMemoryStore();
        final TransactionLog transactionLog = new InMemoryTransactionLog();
        final MasterService masterService = new MasterService(store, transactionLog);
        return new MasterServer(port, masterService);
    }

    private static Server createSlaveServer(int port, ShardConfig config) throws IOException {
        final Store store = new InMemoryStore();
        final SlaveRegistrar slaveRegistrar = new SlaveRegistrar(config.address(), config.masterAddress());
        final SlaveSyncer slaveSyncer = new SlaveSyncer(store, config.masterAddress());
        final SlaveService slaveService = new SlaveService(store, slaveRegistrar, slaveSyncer);
        return new SlaveServer(port, slaveService);
    }
}
