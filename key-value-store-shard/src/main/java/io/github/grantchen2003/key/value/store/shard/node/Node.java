package io.github.grantchen2003.key.value.store.shard.node;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.handlers.*;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.Executors;

public abstract class Node {
    protected final Store store;
    protected final HttpServer server;

    protected Node(int port, Store store) throws IOException {
        this.store = store;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler(store)));
        server.createContext("/put", new LoggingHandler(new PutHandler(this)));
        server.createContext("/delete", new LoggingHandler(new DeleteHandler(this)));
        server.createContext("/health", new LoggingHandler(new HealthHandler(store)));
    }

    public final void start() {
        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
        server.start();
        System.out.println("Key value store shard started on port " + server.getAddress().getPort());

        onStart();
    }

    protected abstract void onStart();
    public abstract void put(String key, String value);
    public abstract Optional<String> remove(String key);

    public static Node create(Role role, InetSocketAddress address, int port, InetSocketAddress masterAddress, Store store, TransactionLog transactionLog) throws IOException {
        return switch (role) {
            case MASTER -> new MasterNode(port, store, transactionLog);
            case SLAVE -> new SlaveNode(address, port, store, masterAddress);
        };
    }
}
