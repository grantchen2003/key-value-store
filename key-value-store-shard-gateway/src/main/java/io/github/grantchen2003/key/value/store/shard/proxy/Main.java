package io.github.grantchen2003.key.value.store.shard.gateway;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.key.value.store.shard.gateway.config.ConfigParser;
import io.github.grantchen2003.key.value.store.shard.gateway.config.GatewayConfig;
import io.github.grantchen2003.key.value.store.shard.gateway.handlers.DeleteHandler;
import io.github.grantchen2003.key.value.store.shard.gateway.handlers.GetHandler;
import io.github.grantchen2003.key.value.store.shard.gateway.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.gateway.handlers.PutHandler;
import io.github.grantchen2003.key.value.store.shard.gateway.service.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException {
        final GatewayConfig config = ConfigParser.parseArgs(args);

        final Service service = new Service(config.masterAddress());

        final HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/get", new LoggingHandler(new GetHandler()));
        server.createContext("/put", new LoggingHandler(new PutHandler(service)));
        server.createContext("/delete", new LoggingHandler(new DeleteHandler(service)));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));

        server.start();
        System.out.println("Shard gateway started on port " + server.getAddress().getPort());
    }
}
