package io.github.grantchen2003.key.value.store.shard.handlers;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;
import java.util.Optional;

public class TransactionLogHandler implements HttpHandler {
    private final Gson gson = new Gson();
    private final MasterService masterService;

    public TransactionLogHandler(MasterService masterService) {
        this.masterService = masterService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        final Optional<Long> startOffsetOpt = extractStartOffset(exchange.getRequestURI());
        if (startOffsetOpt.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        final long startOffset = startOffsetOpt.get();

        final List<Transaction> transactions = masterService.getTransactionsStartingFrom(startOffset);

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream os = exchange.getResponseBody();
             OutputStreamWriter osw = new OutputStreamWriter(os);
             JsonWriter jsonWriter = new JsonWriter(osw)) {

            jsonWriter.beginArray();

            for (Transaction tx : transactions) {
                gson.toJson(tx, Transaction.class, jsonWriter);
                jsonWriter.flush();
            }

            jsonWriter.endArray();
        }
    }

    private Optional<Long> extractStartOffset(URI requestUri) {
        if (requestUri == null) {
            return Optional.empty();
        }

        final String query = requestUri.getQuery();
        if (query == null || !query.startsWith("startOffset=")) {
            return Optional.empty();
        }

        final long startOffset;
        try {
            startOffset = Long.parseLong(query.substring("startOffset=".length()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }

        return Optional.of(startOffset);
    }
}
