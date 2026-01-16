package io.github.grantchen2003.key.value.store.shard.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;

public class LoggingHandler implements HttpHandler {
    private final HttpHandler delegate;

    public LoggingHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // Log request method and URI
        System.out.println("----- Incoming Request -----");
        System.out.println(exchange.getRequestMethod() + " " + exchange.getRequestURI());

        // Log headers
        exchange.getRequestHeaders().forEach((k, v) -> System.out.println(k + ": " + v));

        // Log body if present (read safely)
        InputStream is = exchange.getRequestBody();
        if (is.available() > 0) {
            byte[] bodyBytes = is.readAllBytes();
            System.out.println("Request Body: " + new String(bodyBytes));
            // Pass body to delegate using a new InputStream
            exchange.setStreams(new java.io.ByteArrayInputStream(bodyBytes), exchange.getResponseBody());
        }

        // Call the real handler
        delegate.handle(exchange);

        // Log response code
        System.out.println("----- Response Status: " + exchange.getResponseCode() + " -----");
    }
}
