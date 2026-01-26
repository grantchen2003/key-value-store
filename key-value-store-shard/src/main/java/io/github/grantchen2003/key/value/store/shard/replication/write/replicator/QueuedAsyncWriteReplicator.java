package io.github.grantchen2003.key.value.store.shard.replication.write.replicator;

import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueuedAsyncWriteReplicator implements AsyncWriteReplicator {
    private final Set<InetSocketAddress> slaves = ConcurrentHashMap.newKeySet();
    private final HttpClient client = HttpClient.newHttpClient();
    private final ExecutorService replicationExecutor = Executors.newSingleThreadExecutor(
            r -> {
                Thread t = new Thread(r);
                t.setDaemon(true); // so JVM can exit even if replication tasks are pending
                return t;
            }
    );

    public void addSlave(InetSocketAddress address) {
        slaves.add(address);
    }

    @Override
    public void replicatePutAsync(long txOffset, String key, String value) {
        replicationExecutor.execute(() -> {
            for (InetSocketAddress slave : slaves) {
                replicatePutToSlave(slave, txOffset, key, value);
            }
        });
    }

    @Override
    public void replicateRemoveAsync(long txOffset, String key) {
        replicationExecutor.execute(() -> {
            for (InetSocketAddress slave : slaves) {
                replicateRemoveToSlave(slave, txOffset, key);
            }
        });
    }

    private void replicatePutToSlave(InetSocketAddress slaveAddress, long txOffset, String key, String value) {
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

        client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .exceptionally(ex -> {
                    System.out.println("Replication PUT failed to " + slaveAddress + ": " + ex);
                    return null;
                });
    }

    private void replicateRemoveToSlave(InetSocketAddress slaveAddress, long txOffset, String key) {
        System.out.println("Removing from slave " + slaveAddress);
    }
}
