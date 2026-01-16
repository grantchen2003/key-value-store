package io.github.grantchen2003.key.value.store.shard.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

public class GetHandler implements HttpHandler {
    final Store store;

    public GetHandler(Store store) {
        this.store = store;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final String query = exchange.getRequestURI().getQuery();

        if (!query.startsWith("key=")) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final String key = query.substring(4);

        final Optional<String> value = store.getValue(key);
        if (value.isEmpty()) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        exchange.sendResponseHeaders(200, value.get().getBytes().length);
        final OutputStream os = exchange.getResponseBody();
        os.write(value.get().getBytes());
        os.close();
    }
}
