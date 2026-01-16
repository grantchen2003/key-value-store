package io.github.grantchen2003.request.router;

import com.sun.net.httpserver.HttpServer;
import io.github.grantchen2003.request.router.handlers.DeleteHandler;
import io.github.grantchen2003.request.router.handlers.GetHandler;
import io.github.grantchen2003.request.router.handlers.PutHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException {
        final List<String> shardIps = List.of(System.getenv("SHARD_IPS").split(","));
        final ShardRouter shardRouter = new ShardRouter(shardIps);

        final HttpServer server = HttpServer.create(new InetSocketAddress(8083), 0);

        server.createContext("/get", new GetHandler(shardRouter));
        server.createContext("/put", new PutHandler(shardRouter));
        server.createContext("/delete", new DeleteHandler(shardRouter));

        final int numCores = Runtime.getRuntime().availableProcessors();
        server.setExecutor(Executors.newFixedThreadPool(numCores * 2));

        server.start();
        System.out.println("Key-value store request router started on port 8083");
    }
}
