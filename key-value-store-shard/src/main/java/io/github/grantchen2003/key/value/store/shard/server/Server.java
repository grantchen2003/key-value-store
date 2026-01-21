package io.github.grantchen2003.key.value.store.shard.server;

import io.github.grantchen2003.key.value.store.shard.config.ShardConfig;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveRegistrar;
import io.github.grantchen2003.key.value.store.shard.replication.MasterWriteReplicator;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveSyncer;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;

public interface Server {
    void start();

    static Server create(int port, ShardConfig shardConfig, Store store, TransactionLog transactionLog, MasterWriteReplicator masterWriteReplicator, SlaveRegistrar slaveRegistrar, SlaveSyncer slaveSyncer) throws IOException {
        return switch (shardConfig.role()) {
            case MASTER -> {
                final MasterService masterService = new MasterService(store, transactionLog, masterWriteReplicator);
                yield new MasterServer(port, masterService);
            }
            case SLAVE -> {
                final SlaveService slaveService = new SlaveService(store, slaveRegistrar, slaveSyncer);
                yield new SlaveServer(port, slaveService);
            }
        };
    }
}
