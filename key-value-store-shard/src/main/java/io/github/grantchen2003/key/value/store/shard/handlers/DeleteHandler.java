package io.github.grantchen2003.key.value.store.shard.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class DeleteHandler implements HttpHandler {
    private final MasterService masterService;

    public DeleteHandler(MasterService masterService) {
        this.masterService = masterService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("DELETE")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final Optional<String> keyOpt = extractKey(exchange.getRequestURI());
        if (keyOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final String key = keyOpt.get();

        final Optional<String> value = masterService.delete(key);

        if (value.isEmpty()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        exchange.sendResponseHeaders(200, value.get().getBytes().length);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        try (final OutputStream os = exchange.getResponseBody()) {
            os.write(value.get().getBytes(StandardCharsets.UTF_8));
        }
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
}
