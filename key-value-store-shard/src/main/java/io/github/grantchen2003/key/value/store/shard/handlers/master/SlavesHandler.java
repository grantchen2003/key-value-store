package io.github.grantchen2003.key.value.store.shard.handlers.master;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SlavesHandler implements HttpHandler {
    private final MasterService masterService;

    public SlavesHandler(MasterService masterService) {
        this.masterService = masterService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleListSlaves(exchange);
            case "POST" -> handleAddSlave(exchange);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleListSlaves(HttpExchange exchange) throws IOException {
        final JSONArray slaves = new JSONArray();
        for (final InetSocketAddress slaveAddress : masterService.getSlaveAddresses()) {
            slaves.put(NetworkUtils.toHostPort(slaveAddress));
        }

        final JSONObject response = new JSONObject()
                .put("slaves", slaves);

        final byte[] responseBytes = response.toString().getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, responseBytes.length);
        try (final OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    private void handleAddSlave(HttpExchange exchange) throws IOException {
        final Optional<InetSocketAddress> slaveAddressOpt = extractSlaveAddress(exchange.getRequestURI());
        if (slaveAddressOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final InetSocketAddress slaveAddress = slaveAddressOpt.get();

        masterService.addSlave(slaveAddress);

        exchange.sendResponseHeaders(200, -1);
    }

    private Optional<InetSocketAddress> extractSlaveAddress(URI requestUri) {
        if (requestUri == null) {
            return Optional.empty();
        }

        final String query = requestUri.getQuery();
        if (query == null || !query.startsWith("address=")) {
            return Optional.empty();
        }

        final String slaveAddressStr = query.substring("address=".length());
        final InetSocketAddress slaveAddress = NetworkUtils.parseAddress(slaveAddressStr);
        return Optional.of(slaveAddress);
    }
}
