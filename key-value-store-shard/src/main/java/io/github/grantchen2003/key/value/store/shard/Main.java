package io.github.grantchen2003.key.value.store.shard;

import io.github.grantchen2003.key.value.store.shard.node.Node;
import io.github.grantchen2003.key.value.store.shard.node.Role;
import io.github.grantchen2003.key.value.store.shard.store.InMemoryStore;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        final Map<String, String> config = Config.parseArgs(args);

        final Role role = Role.valueOf(config.get("role"));
        final InetSocketAddress address = NetworkUtils.parseAddress(config.get("address"));
        final int port = 8080;
        final Store store = new InMemoryStore();
        final InetSocketAddress masterAddress = NetworkUtils.parseAddress(config.get("masterAddress"));

        final Node node = Node.create(role, address, port, store, masterAddress);
        node.start();
    }
}
