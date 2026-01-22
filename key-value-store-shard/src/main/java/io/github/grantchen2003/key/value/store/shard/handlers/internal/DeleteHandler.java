package io.github.grantchen2003.key.value.store.shard.handlers.internal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

public class DeleteHandler implements HttpHandler {
    private final SlaveService slaveService;

    public DeleteHandler(SlaveService slaveService) {
        this.slaveService = slaveService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("DELETE")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final Optional<Long> txOffsetOpt = extractTxOffset(exchange.getRequestURI());
        if (txOffsetOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final Optional<String> keyOpt = extractKey(exchange.getRequestURI());
        if (keyOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final long txOffset = txOffsetOpt.get();
        final String key = keyOpt.get();

        slaveService.remove(txOffset, key);

        exchange.sendResponseHeaders(200, -1);
    }

    private Optional<String> extractKey(URI requestUri) {
        if (requestUri == null) {
            return Optional.empty();
        }

        final String query = requestUri.getQuery();
        if (query == null || !query.startsWith("key=")) {
            return Optional.empty();
        }

        return Optional.of(query.substring("key=".length()));
    }

    //TODO
    private Optional<Long> extractTxOffset(URI requestUri) {
        return Optional.empty();
    }
}
