package io.github.grantchen2003.key.value.store.shard.transaction;

public record Transaction(long offset, TransactionType type, String key, String value) {}