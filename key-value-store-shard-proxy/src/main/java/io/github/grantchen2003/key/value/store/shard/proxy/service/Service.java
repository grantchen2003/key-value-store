package io.github.grantchen2003.key.value.store.shard.proxy.service;

import io.github.grantchen2003.key.value.store.shard.proxy.SlaveRegistry;
import io.github.grantchen2003.key.value.store.shard.proxy.utils.NetworkUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Service {
    private final HttpClient client = HttpClient.newHttpClient();
    private final InetSocketAddress masterAddress;
    private final SlaveRegistry slaveRegistry;

    public Service(InetSocketAddress masterAddress, SlaveRegistry slaveRegistry) {
        this.masterAddress = masterAddress;
        this.slaveRegistry = slaveRegistry;
    }

    public GetResult get(String key, boolean isStronglyConsistent) {
        final InetSocketAddress readServer;
        final List<InetSocketAddress> slaveAddresses = slaveRegistry.getSlaveAddresses();
        if (isStronglyConsistent || slaveAddresses.isEmpty()) {
            readServer = masterAddress;
        } else {
            final int randomIndex = ThreadLocalRandom.current().nextInt(slaveAddresses.size());
            readServer = slaveAddresses.get(randomIndex);
        }

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + NetworkUtils.toHostPort(readServer) + "/get?key=" + key))
                .GET()
                .build();

        try {
            final HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            return new GetResult(response.statusCode(), new String(response.body(), StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new GetResult(500, null);
        } catch (IOException e) {
            return new GetResult(500, null);
        }
    }

    public int put(String key, String value) {
        final URI masterUri = URI.create("http://" + NetworkUtils.toHostPort(masterAddress) + "/put");
        System.out.println("Forwarding PUT to master " + masterUri);

        final JSONObject payload = new JSONObject()
                .put("key", key)
                .put("value", value);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(masterUri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                .build();

        try {
            final HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode();
        } catch (InterruptedException | IOException e) {
            return 500;
        }
    }
}
