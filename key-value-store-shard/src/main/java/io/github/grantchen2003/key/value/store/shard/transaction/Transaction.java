package io.github.grantchen2003.key.value.store.shard.transaction;

public abstract class Transaction {
    public final long offset;
    public final String type;

    public Transaction(long offset, String type) {
        this.offset = offset;
        this.type = type;
    }

    public abstract Transaction copy();
}