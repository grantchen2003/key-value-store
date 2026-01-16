package io.github.grantchen2003.request.router;

import java.util.List;

public class ShardRouter {
    private final List<String> shardIps;

    public ShardRouter(List<String> shardIps) {
        if (shardIps == null || shardIps.isEmpty()) {
            throw new IllegalArgumentException("shardIps must not be empty");
        }
        this.shardIps = shardIps;
    }

    public String getShardIp(String key) {
        final int keyHash = Math.abs(key.hashCode());
        final int shardIndex = keyHash % shardIps.size();
        return shardIps.get(shardIndex);
    }
}
