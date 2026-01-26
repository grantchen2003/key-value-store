package io.github.grantchen2003.key.value.store.shard.replication.write.replicator;

import java.net.InetSocketAddress;

public interface AsyncWriteReplicator {
    void addSlave(InetSocketAddress address);
    void replicatePutAsync(long txOffset, String key, String value);
    void replicateRemoveAsync(long txOffset, String key);
}
