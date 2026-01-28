package io.github.grantchen2003.key.value.store.shard.gateway.service;

import io.github.grantchen2003.key.value.store.shard.gateway.utils.NetworkUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Service {
    private final HttpClient client = HttpClient.newHttpClient();
    private final Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();
    private final InetSocketAddress masterAddress;

    public Service(InetSocketAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public int get(String, )

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
