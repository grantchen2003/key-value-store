package io.github.grantchen2003.key.value.store.shard.proxy.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;
import io.github.grantchen2003.key.value.store.shard.proxy.utils.HttpRequestUtils;
import io.github.grantchen2003.key.value.store.shard.proxy.utils.NetworkUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class SlaveHandler implements HttpHandler {
    private final Service service;

    public SlaveHandler(Service service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "PUT" -> handleAddSlave(exchange);
            case "DELETE" -> handleRemoveSlave(exchange);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleAddSlave(HttpExchange exchange) throws IOException {
        final InputStream inputStream = exchange.getRequestBody();
        final String body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        final JSONObject jsonObject = new JSONObject(body);

        final InetSocketAddress slaveAddress;

        try {
            slaveAddress = NetworkUtils.parseAddress(jsonObject.getString("slaveAddress"));
        } catch (JSONException e) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        service.addSlave(slaveAddress);

        exchange.sendResponseHeaders(200, -1);
    }

    private void handleRemoveSlave(HttpExchange exchange) throws IOException {
        final Optional<String> slaveAddressOpt = HttpRequestUtils.getParam(exchange.getRequestURI(), "slaveAddress");
        if (slaveAddressOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final InetSocketAddress slaveAddress = NetworkUtils.parseAddress(slaveAddressOpt.get());

        service.removeSlave(slaveAddress);

        exchange.sendResponseHeaders(200, -1);

    }
}
