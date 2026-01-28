package io.github.grantchen2003.key.value.store.shard.proxy.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

public class LoggingHandler implements HttpHandler {
    private final HttpHandler delegate;

    public LoggingHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        System.out.println("----- Incoming Request -----");
        System.out.println(exchange.getRequestMethod() + " " + exchange.getRequestURI());

        if (exchange.getRequestBody().available() > 0) {
            final byte[] body = exchange.getRequestBody().readAllBytes();
            System.out.println("Request Body: " + new String(body));
            exchange.setStreams(new java.io.ByteArrayInputStream(body), exchange.getResponseBody());
        }

        delegate.handle(exchange);

        System.out.println("----- Response Status: " + exchange.getResponseCode() + " -----");
    }
}
