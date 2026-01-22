package io.github.grantchen2003.key.value.store.shard.transaction;

public class PutTransaction extends Transaction {
    public final String key;
    public final String value;

    public PutTransaction(long offset, String key, String value) {
        super(offset, "PUT");
        this.key = key;
        this.value = value;
    }

    private PutTransaction(PutTransaction other) {
        super(other.offset, other.type);
        this.key = other.key;
        this.value = other.value;
    }

    @Override
    public PutTransaction copy() {
        return new PutTransaction(this);
    }
}