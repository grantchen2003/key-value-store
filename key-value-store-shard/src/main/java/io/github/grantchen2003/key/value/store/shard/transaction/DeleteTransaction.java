package io.github.grantchen2003.key.value.store.shard.transaction;

public class DeleteTransaction extends Transaction {
    public final String type = "DELETE";
    public final String key;

    public DeleteTransaction(long offset, String key) {
        super(offset);
        this.key = key;
    }
}