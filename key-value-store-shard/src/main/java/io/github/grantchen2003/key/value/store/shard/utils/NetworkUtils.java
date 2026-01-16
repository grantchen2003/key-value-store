package io.github.grantchen2003.key.value.store.shard.utils;

import java.net.InetSocketAddress;

public class NetworkUtils {
    public static InetSocketAddress parseAddress(String address) {
        if (address == null || !address.contains(":")) {
            throw new IllegalArgumentException("Address must be in the format host:port");
        }

        final String[] parts = address.split(":");
        final String host = parts[0];
        final int port = Integer.parseInt(parts[1]);

        return new InetSocketAddress(host, port);
    }
}
