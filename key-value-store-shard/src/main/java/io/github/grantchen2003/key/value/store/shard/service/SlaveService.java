package io.github.grantchen2003.key.value.store.shard.service;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionType;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

public class SlaveService {
    private final HttpClient client = HttpClient.newHttpClient();
    private final Store store;
    private final InetSocketAddress address;
    private final InetSocketAddress masterAddress;
    private long lastAppliedTxOffset = 0;

    public SlaveService(Store store, InetSocketAddress address, InetSocketAddress masterAddress) {
        this.store = store;
        this.address = address;
        this.masterAddress = masterAddress;
    }

    public Optional<String> get(String key) {
        return store.getValue(key);
    }

    public void put(Transaction tx) {
        if (tx.offset() == lastAppliedTxOffset + 1) {
            store.put(tx.key(), tx.value());
            lastAppliedTxOffset++;
        } else if (tx.offset() > lastAppliedTxOffset + 1) {
            // slave is missing [lastAppliedTxOffset, tx.offset() - 1]
        }
    }

    public Optional<String> remove(String key) {
        return store.remove(key);
    }

    public void start() {
        System.out.println("Registering with master...");
        registerWithMaster();
        System.out.println("Successfully registered with master.");

        System.out.println("Syncing with master...");
        syncWithMaster();
        System.out.println("Successfully synced with master.");
    }

    private void registerWithMaster() {
        final URI shardUri = URI.create("http://" + NetworkUtils.toHostPort(masterAddress) + "/slave?address=" + NetworkUtils.toHostPort(address));
        System.out.println("Sending POST request to register with master at " + shardUri);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(shardUri)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        try {
            final HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to register slave. HTTP " + response.statusCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error registering slave with master", e);
        }
    }

    // TODO: KEEP SYNCING WITH POLLING UNTIL LAST APPLIED TX TO SLAVE IS THE SAME AS MASTER. THEN ONLY RELY ON MASTER WRITE PROPAGATIONS TO BE IN SYNC
    private void syncWithMaster() {
        final URI masterUri = URI.create("http://" + NetworkUtils.toHostPort(masterAddress) + "/transaction-log?startOffset=" + lastAppliedTxOffset);
        System.out.println("Sending POST request to register with master at " + masterUri);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(masterUri)
                .GET()
                .build();

        try {
            final HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to sync with master, status code: " + response.statusCode());
            }

            try (
                    final InputStreamReader reader = new InputStreamReader(response.body());
                    final JsonReader jsonReader = new JsonReader(reader)
            ) {
                final Gson gson = new Gson();
                jsonReader.beginArray();

                while (jsonReader.hasNext()) {
                    final Transaction tx = gson.fromJson(jsonReader, Transaction.class);
                    processReplicationLogEntry(tx);
                }

                jsonReader.endArray();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error syncing slave with master", e);
        }
    }

    private void processReplicationLogEntry(Transaction tx) {
        switch (tx.type()) {
            case TransactionType.PUT -> store.put(tx.key(), tx.value());
            case TransactionType.DELETE -> store.remove(tx.key());
        }
        lastAppliedTxOffset++;
    }
}
