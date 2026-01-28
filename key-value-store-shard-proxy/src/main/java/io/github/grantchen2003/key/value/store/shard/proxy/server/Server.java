package io.github.grantchen2003.key.value.store.shard.proxy.server;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.proxy.handlers.GetHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.handlers.PutHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class Server {
    private final HttpServer server;

    public Server(int port, Service service) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler(service)));
        server.createContext("/put", new LoggingHandler(new PutHandler(service)));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
    }

    public void start() {
        server.start();
        System.out.println("Shard proxy started on port " + server.getAddress().getPort());
    }
}
