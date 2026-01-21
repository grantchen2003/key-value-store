package io.github.grantchen2003.key.value.store.shard.server;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.handlers.common.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.internal.DeleteHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.internal.GetHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.internal.PutHandler;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SlaveServer implements Server {
    private final HttpServer server;
    private final SlaveService slaveService;

    public SlaveServer(int port, SlaveService slaveService) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/internal/get", new LoggingHandler(new GetHandler(slaveService)));
        server.createContext("/internal/put", new LoggingHandler(new PutHandler(slaveService)));
        server.createContext("/internal/delete", new LoggingHandler(new DeleteHandler(slaveService)));
        this.slaveService = slaveService;
    }

    public void start() {
        server.start();
        System.out.println("Slave shard server started on port " + server.getAddress().getPort());

        slaveService.start();
    }
}
