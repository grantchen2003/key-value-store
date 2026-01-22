package io.github.grantchen2003.key.value.store.shard.transaction;

public class DeleteTransaction extends Transaction {
    public final String key;

    public DeleteTransaction(long offset, String key) {
        super(offset, "DELETE");
        this.key = key;
    }

    private DeleteTransaction(DeleteTransaction other) {
        super(other.offset, other.type);
        this.key = other.key;
    }

    @Override
    public DeleteTransaction copy() {
        return new DeleteTransaction(this);
    }
}