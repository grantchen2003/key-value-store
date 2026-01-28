package io.github.grantchen2003.key.value.store.shard.handlers.internal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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

        final Map<String, String> queryParams = getQueryParams(exchange.getRequestURI());
        if (queryParams.get("txOffset") == null || queryParams.get("key") == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final long txOffset;
        try {
            txOffset = Long.parseLong(queryParams.get("txOffset"));
        } catch (NumberFormatException e) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final String key = queryParams.get("key");

        slaveService.delete(txOffset, key);

        exchange.sendResponseHeaders(200, -1);
    }

    private Map<String, String> getQueryParams(URI requestUri) {
        final Map<String, String> queryParams = new HashMap<>();
        if (requestUri == null) {
            return queryParams;
        }

        final String query = requestUri.getQuery();
        for (final String pair : query.split("&")) {
            if (pair.startsWith("txOffset=")) {
                final String txOffset = pair.substring("txOffset=".length());
                if (!txOffset.isEmpty()) {
                    queryParams.put("txOffset", txOffset);
                }
            }
            if (pair.startsWith("key=")) {
                final String key = pair.substring("key=".length());
                if (!key.isEmpty()) {
                    queryParams.put("key", key);
                }
            }
        }
        return queryParams;
    }
}
