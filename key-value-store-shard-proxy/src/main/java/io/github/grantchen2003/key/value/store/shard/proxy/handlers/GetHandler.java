package io.github.grantchen2003.key.value.store.shard.proxy.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.service.GetResult;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;

import java.io.IOException;
import java.io.OutputStream;

public class GetHandler implements HttpHandler {
    private final Service service;

    public GetHandler(Service service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final String query = exchange.getRequestURI().getQuery();

        final String key = getParameter(query, "key");
        if (key == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final String consistencyParam = getParameter(query, "is_strongly_consistent");
        final boolean isStronglyConsistent = "true".equalsIgnoreCase(consistencyParam);

        final GetResult getResult = service.get(key, isStronglyConsistent);

        exchange.sendResponseHeaders(200, getResult.value().getBytes().length);
        try (final OutputStream os = exchange.getResponseBody()) {
            os.write(getResult.value().getBytes());
        }
    }

    private String getParameter(String query, String paramName) {
        if (query == null || paramName == null) return null;

        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            String key = idx > 0 ? pair.substring(0, idx) : pair;
            if (key.equals(paramName)) {
                return idx > 0 && pair.length() > idx + 1
                        ? pair.substring(idx + 1)
                        : null;
            }
        }
        return null;
    }
}
