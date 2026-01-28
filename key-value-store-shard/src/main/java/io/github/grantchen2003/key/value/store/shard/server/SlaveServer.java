package io.github.grantchen2003.key.value.store.shard.server;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.handlers.common.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.internal.GetHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.internal.ReplicationHandler;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class SlaveServer implements Server {
    private final HttpServer server;
    private final SlaveService slaveService;

    public SlaveServer(int port, SlaveService slaveService) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler(slaveService)));
        server.createContext("/replicate", new LoggingHandler(new ReplicationHandler(slaveService)));
        this.slaveService = slaveService;

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));
    }

    public void start() {
        server.start();
        System.out.println("Slave shard server started on port " + server.getAddress().getPort());

        slaveService.start();
    }
}
