package io.github.grantchen2003.key.value.store.shard.store;

import java.util.HashMap;
import java.util.Optional;

public class InMemoryStore implements Store {
    final HashMap<String, String> data = new HashMap<>();

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
    public Optional<String> removeKey(String key) {
        return Optional.ofNullable(data.remove(key));
    }
}
