package io.github.grantchen2003.key.value.store.shard.handlers.master;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PutHandler implements HttpHandler {
    private final MasterService masterService;

    public PutHandler(MasterService masterService) {
        this.masterService = masterService;
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

        masterService.put(key, value);

        exchange.sendResponseHeaders(200, -1);
    }
}
