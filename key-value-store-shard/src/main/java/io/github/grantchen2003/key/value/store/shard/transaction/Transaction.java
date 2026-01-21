package io.github.grantchen2003.key.value.store.shard.transaction;
// Add a String "type" field
public abstract class Transaction {
    public final long offset;

    public Transaction(long offset) {
        this.offset = offset;
    }
}