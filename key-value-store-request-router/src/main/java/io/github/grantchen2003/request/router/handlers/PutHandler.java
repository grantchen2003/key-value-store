package io.github.grantchen2003.request.router.handlers;

import com.sun.net.httpserver.HttpExchange;
import io.github.grantchen2003.request.router.ShardRouter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class PutHandler extends AbstractShardHandler {
    public PutHandler(ShardRouter router) {
        super(router);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

        final String key, value;
        try {
            final JSONObject jsonObject = new JSONObject(body);
            key = jsonObject.getString("key");
            value = jsonObject.getString("value");
        } catch (JSONException e) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final JSONObject payload = new JSONObject()
                .put("key", key)
                .put("value", value);

        final String shardIp = shardRouter.getShardIp(key);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + shardIp + "/put"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                .build();

        try {
            final HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            exchange.sendResponseHeaders(response.statusCode(), -1);

            System.out.println("POST " + request.uri() + " | Response: " + response.statusCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(500, -1);
        } catch (IOException e) {
            exchange.sendResponseHeaders(500, -1);
        }
    }
}
