package io.github.grantchen2003.key.value.store.shard.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;

public class HealthHandler implements HttpHandler {
    final Store store;
    public HealthHandler(Store store) {
        this.store = store;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // Track number of keys if you have a Store object
        int numKeys = store.size();

        // JVM uptime
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        long uptimeMs = runtimeBean.getUptime();

        // CPU load
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double systemLoad = osBean.getSystemLoadAverage(); // system load avg (1 min)

        // Memory usage
        long freeMemBytes = Runtime.getRuntime().freeMemory();
        long totalMemBytes = Runtime.getRuntime().totalMemory();

        // Build JSON response
        String response = String.format(
                "{\"status\":\"OK\",\"uptimeMs\":%d,\"numKeys\":%d,\"cpuLoad\":%.2f,\"memoryUsedBytes\":%d,\"memoryTotalBytes\":%d}",
                uptimeMs, numKeys, systemLoad, totalMemBytes - freeMemBytes, totalMemBytes
        );

        exchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }

    }
}
