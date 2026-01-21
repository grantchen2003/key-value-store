package io.github.grantchen2003.key.value.store.shard.transaction;

public abstract class Transaction {
    public final long offset;

    public Transaction(long offset) {
        this.offset = offset;
    }
}