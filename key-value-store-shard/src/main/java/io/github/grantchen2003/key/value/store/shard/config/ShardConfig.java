package io.github.grantchen2003.key.value.store.shard.config;

import java.net.InetSocketAddress;

public record ShardConfig(Role role, InetSocketAddress masterAddress, InetSocketAddress address) {}
