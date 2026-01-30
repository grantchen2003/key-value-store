package io.github.grantchen2003.key.value.store.shard.proxy.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.service.GetResult;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;
import io.github.grantchen2003.key.value.store.shard.proxy.utils.HttpRequestUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

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

        final Optional<String> keyOpt = HttpRequestUtils.getParam(exchange.getRequestURI(), "key");
        if (keyOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final Optional<String> isStronglyConsistentOpt = HttpRequestUtils.getParam(exchange.getRequestURI(), "is_strongly_consistent");
        if (isStronglyConsistentOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final boolean isStronglyConsistent = "true".equalsIgnoreCase(isStronglyConsistentOpt.get());

        final GetResult getResult = service.get(keyOpt.get(), isStronglyConsistent);

        if (getResult.valueOpt().isEmpty()) {
            exchange.sendResponseHeaders(getResult.statusCode(), -1);
            return;
        }

        final byte[] valueBytes = getResult.valueOpt().get().getBytes();
        exchange.sendResponseHeaders(getResult.statusCode(), valueBytes.length == 0 ? -1 : valueBytes.length);
        try (final OutputStream os = exchange.getResponseBody()) {
            os.write(valueBytes);
        }
    }
}
