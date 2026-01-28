package io.github.grantchen2003.key.value.store.shard.gateway.config;

import io.github.grantchen2003.key.value.store.shard.gateway.utils.NetworkUtils;

import java.net.InetSocketAddress;

public class ConfigParser {
    public static GatewayConfig parseArgs(String[] args) {
        InetSocketAddress masterAddress = null;

        for (final String arg : args) {
            if (arg.startsWith("--masterAddress=")) {
                masterAddress = NetworkUtils.parseAddress(arg.substring("--masterAddress=".length()));
            }
        }
        if (masterAddress == null) {
            throw new RuntimeException("Missing --masterAddress argument");
        }

        return new GatewayConfig(masterAddress);
    }
}
