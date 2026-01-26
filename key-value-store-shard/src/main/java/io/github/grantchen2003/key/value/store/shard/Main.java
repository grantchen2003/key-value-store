package io.github.grantchen2003.key.value.store.shard;

import io.github.grantchen2003.key.value.store.shard.config.ConfigParser;
import io.github.grantchen2003.key.value.store.shard.config.ShardConfig;
import io.github.grantchen2003.key.value.store.shard.server.Server;
import io.github.grantchen2003.key.value.store.shard.server.ServerFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        final ShardConfig config = ConfigParser.parseArgs(args);
        final Server server = ServerFactory.createServer(config.address().getPort(), config);
        server.start();
    }
}
