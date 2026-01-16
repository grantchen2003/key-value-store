package io.github.grantchen2003.key.value.store.shard.store;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStore implements Store {
    final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public void put(String key, String value) {
        data.put(key, value);
    }

    @Override
    public Optional<String> remove(String key) {
        return Optional.ofNullable(data.remove(key));
    }
}
