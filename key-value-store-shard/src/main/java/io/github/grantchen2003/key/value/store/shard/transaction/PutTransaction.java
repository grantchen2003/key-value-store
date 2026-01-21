package io.github.grantchen2003.key.value.store.shard.transaction;

public class PutTransaction extends Transaction {
    public final String type = "PUT";
    public final String key;
    public final String value;

    public PutTransaction(long offset, String key, String value) {
        super(offset);
        this.key = key;
        this.value = value;
    }
}