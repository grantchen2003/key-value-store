package io.github.grantchen2003.key.value.store.shard.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;

public class SlaveHandler implements HttpHandler {
    final MasterService masterService;

    public SlaveHandler(MasterService masterService) {
        this.masterService = masterService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final Optional<InetSocketAddress> slaveAddressOpt = extractSlaveAddress(exchange.getRequestURI());
        if (slaveAddressOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final InetSocketAddress slaveAddress = slaveAddressOpt.get();

        masterService.addSlaveAddress(slaveAddress);

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
