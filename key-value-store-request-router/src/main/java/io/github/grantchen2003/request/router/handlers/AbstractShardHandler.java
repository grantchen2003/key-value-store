package io.github.grantchen2003.request.router.handlers;

import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.request.router.ShardRouter;

import java.net.http.HttpClient;

public abstract class AbstractShardHandler implements HttpHandler {
    protected final ShardRouter shardRouter;
    protected static final HttpClient client = HttpClient.newHttpClient();

    protected AbstractShardHandler(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }
}
