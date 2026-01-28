package io.github.grantchen2003.key.value.store.shard.proxy.config;

import io.github.grantchen2003.key.value.store.shard.proxy.utils.NetworkUtils;

import java.net.InetSocketAddress;

public class ConfigParser {
    public static ShardProxyConfig parseArgs(String[] args) {
        InetSocketAddress address = null;
        InetSocketAddress masterAddress = null;

        for (final String arg : args) {
            if (arg.startsWith("--address=")) {
                address = NetworkUtils.parseAddress(arg.substring("--address=".length()));
            } else if (arg.startsWith("--masterAddress=")) {
                masterAddress = NetworkUtils.parseAddress(arg.substring("--masterAddress=".length()));
            }
        }

        if (address == null) {
            throw new RuntimeException("Missing --address argument");
        }

        if (masterAddress == null) {
            throw new RuntimeException("Missing --masterAddress argument");
        }

        return new ShardProxyConfig(address, masterAddress);
    }
}
