package io.github.grantchen2003.key.value.store.shard.transaction;

import java.io.Serializable;

public abstract class Transaction implements Serializable {
    public final long offset;
    public final String type;

    public Transaction(long offset, String type) {
        this.offset = offset;
        this.type = type;
    }

    public abstract Transaction copy();
}