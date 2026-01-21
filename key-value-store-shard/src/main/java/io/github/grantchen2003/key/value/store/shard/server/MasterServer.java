package io.github.grantchen2003.key.value.store.shard.server;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.handlers.DeleteHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.GetHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.common.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.PutHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.TransactionLogHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.SlaveHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class MasterServer implements Server {
    private final HttpServer server;

    public MasterServer(int port, MasterService masterService) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler(masterService)));
        server.createContext("/put", new LoggingHandler(new PutHandler(masterService)));
        server.createContext("/delete", new LoggingHandler(new DeleteHandler(masterService)));
        server.createContext("/slave", new LoggingHandler(new SlaveHandler(masterService)));
        server.createContext("/transaction-log", new LoggingHandler(new TransactionLogHandler(masterService)));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
    }

    public void start() {
        server.start();
        System.out.println("Master shard server started on port " + server.getAddress().getPort());
    }
}
