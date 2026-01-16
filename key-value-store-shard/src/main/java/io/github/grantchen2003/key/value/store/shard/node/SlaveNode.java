package io.github.grantchen2003.key.value.store.shard.node;

import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

public class SlaveNode extends Node {
    private final InetSocketAddress address;
    private final InetSocketAddress masterAddress;

    public SlaveNode(InetSocketAddress address, int port, Store store, InetSocketAddress masterAddress) throws IOException {
        super(port, store);
        this.address = address;
        this.masterAddress = masterAddress;
    }

    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    @Override
    public Optional<String> remove(String key) {
        return store.remove(key);
    }

    @Override
    protected void onStart() {
        System.out.println("Slave node started");
        System.out.println("Registering with master...");
        registerWithMaster();
        System.out.println("Successfully registered with master...");
    }

    private void registerWithMaster() {
        final URI shardUri = URI.create("http://" + NetworkUtils.toHostPort(masterAddress) + "/slave?address=" + NetworkUtils.toHostPort(address));
        System.out.println("Sending POST request to master at " + shardUri);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(shardUri)
                .timeout(Duration.ofSeconds(3))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        try (final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to register slave. HTTP " + response.statusCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error registering slave with master", e);
        }
    }
}
