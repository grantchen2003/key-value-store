package io.github.grantchen2003.key.value.store.shard.proxy.config;

import java.net.InetSocketAddress;

public record ShardProxyConfig(InetSocketAddress masterAddress) {}
