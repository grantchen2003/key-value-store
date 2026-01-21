package io.github.grantchen2003.key.value.store.shard.replication;

import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MasterWriteReplicator {
    private final Set<InetSocketAddress> slaves = ConcurrentHashMap.newKeySet();
    private final HttpClient client = HttpClient.newHttpClient();

    public void addSlave(InetSocketAddress address) {
        slaves.add(address);
    }

    public void replicatePutToSlaves(long txOffset, String key, String value) {
        for (final InetSocketAddress slave : slaves) {
            sendPut(slave, txOffset, key, value);
        }
    }

    public void replicateRemoveToSlaves(long txOffset, String key) {
        for (final InetSocketAddress slave : slaves) {
            sendRemove(slave, txOffset, key);
        }
    }

    private void sendPut(InetSocketAddress slaveAddress, long txOffset, String key, String value) {
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

        // wtf is this try catch
        try {
            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sendRemove(InetSocketAddress slaveAddress, long txOffset, String key) {
        System.out.println("Removing from replica " + slaveAddress);
    }
}
