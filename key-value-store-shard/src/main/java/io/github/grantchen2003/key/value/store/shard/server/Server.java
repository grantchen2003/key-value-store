package io.github.grantchen2003.key.value.store.shard.server;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.config.ShardConfig;
import io.github.grantchen2003.key.value.store.shard.handlers.DeleteHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.GetHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.PutHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.service.Service;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public abstract class Server {
    protected final HttpServer server;

    protected Server(int port, Service service) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler(service)));
        server.createContext("/put", new LoggingHandler(new PutHandler(service)));
        server.createContext("/delete", new LoggingHandler(new DeleteHandler(service)));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
    }

    public abstract void start();

    public static Server create(int port, ShardConfig shardConfig, Store store, TransactionLog transactionLog) throws IOException {
        return switch (shardConfig.role()) {
            case MASTER -> new MasterServer(port, new MasterService(store, transactionLog));
            case SLAVE -> new SlaveServer(port, new SlaveService(store, shardConfig.address(), shardConfig.masterAddress()));
        };
    }
}
