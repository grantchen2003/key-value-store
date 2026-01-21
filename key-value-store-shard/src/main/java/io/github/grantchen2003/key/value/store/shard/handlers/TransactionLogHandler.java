package io.github.grantchen2003.key.value.store.shard.handlers;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;
import io.github.grantchen2003.key.value.store.shard.transaction.DeleteTransaction;
import io.github.grantchen2003.key.value.store.shard.transaction.PutTransaction;
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
        try (final OutputStream os = exchange.getResponseBody();
             final OutputStreamWriter osw = new OutputStreamWriter(os);
             final JsonWriter jsonWriter = new JsonWriter(osw)) {

            jsonWriter.beginArray();

            for (final Transaction tx : transactions) {
                if (tx instanceof PutTransaction putTx) {
                    jsonWriter.beginObject();
                    jsonWriter.name("type").value(putTx.type);
                    jsonWriter.name("offset").value(putTx.offset);
                    jsonWriter.name("key").value(putTx.key);
                    jsonWriter.name("value").value(putTx.value);
                    jsonWriter.endObject();
                } else if (tx instanceof DeleteTransaction delTx) {
                    jsonWriter.beginObject();
                    jsonWriter.name("type").value(delTx.type);
                    jsonWriter.name("offset").value(delTx.offset);
                    jsonWriter.name("key").value(delTx.key);
                    jsonWriter.endObject();
                }
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
