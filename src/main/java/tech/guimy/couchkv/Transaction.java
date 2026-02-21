package tech.guimy.couchkv;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a transaction in the KV store.
 * Provides ACID guarantees with write-ahead logging.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Transaction<K extends Serializable & Comparable<K>, V extends Serializable> {
    
    private final long txId;
    private final KVStore<K, V> store;
    private TxStatus status;

    Transaction(long txId, KVStore<K, V> store) {
        this.txId = txId;
        this.store = store;
        this.status = TxStatus.ACTIVE;
    }

    /**
     * @return the transaction ID
     */
    public long getTxId() {
        return txId;
    }

    /**
     * @return the transaction status
     */
    public TxStatus getStatus() {
        return status;
    }

    /**
     * Gets a value within this transaction
     */
    public V get(K key) {
        checkActive();
        return store.getInternal(this, key);
    }

    /**
     * Sets a value within this transaction
     */
    public void put(K key, V value) {
        checkActive();
        store.putInternal(this, key, value);
    }

    /**
     * Deletes a key within this transaction
     */
    public void delete(K key) {
        checkActive();
        store.deleteInternal(this, key);
    }

    /**
     * Commits this transaction
     */
    public void commit() throws IOException {
        checkActive();
        store.commitInternal(this);
        status = TxStatus.COMMITTED;
    }

    /**
     * Aborts this transaction
     */
    public void abort() throws IOException {
        if (status == TxStatus.ACTIVE) {
            store.abortInternal(this);
            status = TxStatus.ABORTED;
        }
    }

    /**
     * @return true if this transaction is active
     */
    public boolean isActive() {
        return status == TxStatus.ACTIVE;
    }

    private void checkActive() {
        if (status != TxStatus.ACTIVE) {
            throw new IllegalStateException("Transaction is not active: " + status);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction<?, ?> that = (Transaction<?, ?>) o;
        return txId == that.txId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(txId);
    }
}
