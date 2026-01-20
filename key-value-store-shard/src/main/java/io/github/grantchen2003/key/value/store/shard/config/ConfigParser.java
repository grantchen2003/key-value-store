package io.github.grantchen2003.key.value.store.shard.config;

import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.net.InetSocketAddress;

public class ConfigParser {
    public static ShardConfig parseArgs(String[] args) {
        Role role = null;
        InetSocketAddress masterAddress = null;
        InetSocketAddress address = null;

        for (final String arg : args) {
            if (arg.startsWith("--role=")) {
                role = Role.valueOf(arg.substring("--role=".length()));
            } else if (arg.startsWith("--masterAddress=")) {
                masterAddress = NetworkUtils.parseAddress(arg.substring("--masterAddress=".length()));
            } else if (arg.startsWith("--address=")) {
                address = NetworkUtils.parseAddress(arg.substring("--address=".length()));
            }
        }

        if (role == null) {
            throw new RuntimeException("Missing --role argument");
        }

        if (masterAddress == null) {
            throw new RuntimeException("Missing --masterAddress argument");
        }

        if (address == null) {
            throw new RuntimeException("Missing --address argument");
        }

        return new ShardConfig(role, masterAddress, address);
    }
}
