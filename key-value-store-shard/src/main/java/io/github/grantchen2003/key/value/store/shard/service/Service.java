package io.github.grantchen2003.key.value.store.shard.service;

import io.github.grantchen2003.key.value.store.shard.store.Store;

import java.util.Optional;

public abstract class Service {
    protected final Store store;

    public Service(Store store) {
        this.store = store;
    }

    public abstract void put(String key, String value);

    public abstract Optional<String> remove(String key);

    public Optional<String> get(String key) {
        return store.getValue(key);
    }
}
