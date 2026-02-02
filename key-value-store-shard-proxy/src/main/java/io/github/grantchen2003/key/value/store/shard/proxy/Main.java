package io.github.grantchen2003.key.value.store.shard.proxy;

import io.github.grantchen2003.key.value.store.shard.proxy.config.ConfigParser;
import io.github.grantchen2003.key.value.store.shard.proxy.config.ShardProxyConfig;
import io.github.grantchen2003.key.value.store.shard.proxy.registry.SlaveRegistry;
import io.github.grantchen2003.key.value.store.shard.proxy.server.Server;
import io.github.grantchen2003.key.value.store.shard.proxy.service.Service;

import java.io.IOException;

// TODO: Add delete handler
public class Main {
    public static void main(String[] args) throws IOException {
        final ShardProxyConfig config = ConfigParser.parseArgs(args);
        final SlaveRegistry slaveRegistry = new SlaveRegistry(config.masterAddress());
        final Service service = new Service(config.masterAddress(), slaveRegistry);
        final Server server = new Server(config.address().getPort(), service);
        server.start();
    }
}
