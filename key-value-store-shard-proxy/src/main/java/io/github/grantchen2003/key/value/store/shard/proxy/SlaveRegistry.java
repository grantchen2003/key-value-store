package io.github.grantchen2003.key.value.store.shard.proxy;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SlaveRegistry {
    private final InetSocketAddress masterAddress;
    private Set<InetSocketAddress> slaveAddresses = ConcurrentHashMap.newKeySet();
    private final ExpiringSet<InetSocketAddress> blackListedSlaveAddresses = new ExpiringSet<>();

    public SlaveRegistry(InetSocketAddress masterAddress) {
        this.masterAddress = masterAddress;
        startPolling();
    }

    public List<InetSocketAddress> getAvailableSlaveAddresses() {
        return slaveAddresses.stream()
                .filter(address -> !blackListedSlaveAddresses.contains(address))
                .toList();
    }

    public void remove(InetSocketAddress slaveAddress) {
        slaveAddresses.remove(slaveAddress);
    }

    public void add(InetSocketAddress slaveAddress) {
        slaveAddresses.add(slaveAddress);
    }

    public void blackList(InetSocketAddress slaveAddress, Duration duration) {
        blackListedSlaveAddresses.put(slaveAddress, duration);
    }

    private void startPolling() {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());

        scheduler.scheduleAtFixedRate(() -> {
            try {
                syncSlavesWithMaster();
            } catch (Exception e) {
                System.err.println("REGISTRY: Failed to update slave list: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    private void syncSlavesWithMaster() {
        System.out.println("REGISTRY: Polling master at " + masterAddress);

        HttpURLConnection conn = null;
        try {
            final URI uri = new URI("http://" + masterAddress.getHostString() + ":" + masterAddress.getPort() + "/slaves");
            conn = (HttpURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);

            final int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("REGISTRY: Master returned error code " + responseCode);
                return;
            }

            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                final String rawJson = reader.lines().collect(Collectors.joining());
                this.slaveAddresses = parseSlaveAddresses(rawJson);
                System.out.println("REGISTRY: Successfully updated slave list: " + this.slaveAddresses);
            }

        } catch (URISyntaxException e) {
            System.err.println("REGISTRY: Invalid Master URI configuration: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("REGISTRY: Connectivity issue with Master (" + masterAddress + "): " + e.getMessage());
        } catch (Exception e) {
            System.err.println("REGISTRY: Unexpected error during poll: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static Set<InetSocketAddress> parseSlaveAddresses(String rawJson) {
        final JSONObject response = new JSONObject(rawJson);
        final JSONArray slavesArray = response.getJSONArray("slaves");

        final Set<InetSocketAddress> newSlaveAddresses = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < slavesArray.length(); i++) {
            final String hostPort = slavesArray.getString(i);
            final String[] parts = hostPort.split(":");
            newSlaveAddresses.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
        }
        return newSlaveAddresses;
    }
}