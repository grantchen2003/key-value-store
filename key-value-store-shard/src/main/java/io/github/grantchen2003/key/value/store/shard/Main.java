package io.github.grantchen2003.key.value.store.shard;

import io.github.grantchen2003.key.value.store.shard.config.ConfigParser;
import io.github.grantchen2003.key.value.store.shard.config.ShardConfig;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveRegistrar;
import io.github.grantchen2003.key.value.store.shard.replication.MasterWriteReplicator;
import io.github.grantchen2003.key.value.store.shard.replication.SlaveSyncer;
import io.github.grantchen2003.key.value.store.shard.server.Server;
import io.github.grantchen2003.key.value.store.shard.store.InMemoryStore;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.InMemoryTransactionLog;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        final int port = 8080;
        final ShardConfig shardConfig = ConfigParser.parseArgs(args);
        final Store store = new InMemoryStore();
        final TransactionLog transactionLog = new InMemoryTransactionLog();
        final MasterWriteReplicator masterWriteReplicator = new MasterWriteReplicator();
        final SlaveRegistrar slaveRegistrar = new SlaveRegistrar(shardConfig.address(), shardConfig.masterAddress());
        final SlaveSyncer slaveSyncer = new SlaveSyncer(store, shardConfig.masterAddress());

        final Server server = Server.create(port, shardConfig, store, transactionLog, masterWriteReplicator, slaveRegistrar, slaveSyncer);
        server.start();
    }
}
