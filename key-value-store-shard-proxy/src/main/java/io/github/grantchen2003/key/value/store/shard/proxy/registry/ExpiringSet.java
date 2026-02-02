package io.github.grantchen2003.key.value.store.shard.proxy.registry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExpiringSet<T> {
    private final Map<T, Long> map = new ConcurrentHashMap<>();

    public void put(T value, Duration ttl) {
        final long expirationTime = System.currentTimeMillis() + ttl.toMillis();
        map.put(value, expirationTime);
    }

    public boolean contains(T value) {
        final Long expirationTime = map.get(value);
        if (expirationTime == null) {
            return false;
        }

        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > expirationTime) {
            map.remove(value);
            return false;
        }

        return true;
    }
}
