package io.github.grantchen2003.key.value.store.shard.replication;

import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class SlaveRegistrar {
    private static final HttpClient client = HttpClient.newHttpClient();
    private final InetSocketAddress address;
    private final InetSocketAddress masterAddress;

    public SlaveRegistrar(InetSocketAddress address, InetSocketAddress masterAddress) {
        this.address = address;
        this.masterAddress = masterAddress;
    }

    public void register() {
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
}
