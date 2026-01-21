package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionType;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MasterService {
    private static final HttpClient client = HttpClient.newHttpClient();
    private final Store store;
    private final TransactionLog transactionLog;
    private final Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();

    public MasterService(Store store, TransactionLog transactionLog) {
        this.store = store;
        this.transactionLog = transactionLog;
    }

    public synchronized Optional<String> get(String key) {
        return store.getValue(key);
    }

    public synchronized void put(String key, String value) {
        final long txOffset = transactionLog.append(TransactionType.PUT, key, value);

        store.put(key, value);

        // do this async since it will be eventually consistent
        for (final InetSocketAddress slaveAddress : slaveAddresses) {
            replicatePut(slaveAddress, txOffset, key, value);
        }
    }

    public synchronized Optional<String> remove(String key) {
        final long txOffset = transactionLog.append(TransactionType.DELETE, key, null);

        final Optional<String> valueOpt = store.remove(key);

        // TODO: make this concurrent, learn more about java concurrency first
        for (final InetSocketAddress slaveAddress : slaveAddresses) {
            replicateRemove(slaveAddress, txOffset, key);
        }

        return valueOpt;
    }

    public void addSlaveAddress(InetSocketAddress slaveAddress) {
        slaveAddresses.add(slaveAddress);
    }

    public List<Transaction> getTransactionsStartingFrom(long startOffset) {
        return transactionLog.getTransactionsStartingFrom(startOffset);
    }

    private void replicatePut(InetSocketAddress slaveAddress, long txOffset, String key, String value) {
        final URI slaveUri = URI.create("http://" + NetworkUtils.toHostPort(slaveAddress) + "/internal/put");
        System.out.println("Putting to slave " + slaveUri);

        final JSONObject payload = new JSONObject()
                .put("txOffset", txOffset)
                .put("key", key)
                .put("value", value);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(slaveUri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

        private void replicateRemove(InetSocketAddress slaveAddress, long txOffset, String key) {
        System.out.println("Removing from replica " + slaveAddress);
    }
}
