package io.github.grantchen2003.key.value.store.shard.store;

import java.util.Optional;

public interface Store {
    int size();
    Optional<String> getValue(String key);
    void put(String key, String value);
    Optional<String> removeKey(String key);
}
