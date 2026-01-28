package io.github.grantchen2003.key.value.store.shard.proxy.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PutHandler implements HttpHandler {
    private final Service service;

    public PutHandler(Service service) {
        this.service = service;
    }
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final InputStream inputStream = exchange.getRequestBody();
        final String body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        final JSONObject jsonObject = new JSONObject(body);

        final String key, value;

        try {
            key = jsonObject.getString("key");
            value = jsonObject.getString("value");
        } catch (JSONException e) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final int statusCode = service.put(key, value);

        exchange.sendResponseHeaders(statusCode, -1);
    }
}
