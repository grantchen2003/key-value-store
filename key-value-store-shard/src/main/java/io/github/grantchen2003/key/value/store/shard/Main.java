package io.github.grantchen2003.key.value.store.shard;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.handlers.DeleteHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.GetHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.HealthHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.PutHandler;
import io.github.grantchen2003.key.value.store.shard.store.InMemoryStore;
import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException {
        final Store store = new InMemoryStore();

        final HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/get", new LoggingHandler(new GetHandler(store)));
        server.createContext("/put", new LoggingHandler(new PutHandler(store)));
        server.createContext("/delete", new LoggingHandler(new DeleteHandler(store)));
        server.createContext("/health", new LoggingHandler(new HealthHandler(store)));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
        server.start();

        System.out.println("Key-value store shard started on port 8080");
    }
}
