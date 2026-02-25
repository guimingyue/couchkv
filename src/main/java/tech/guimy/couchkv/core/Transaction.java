package tech.guimy.couchkv.core;

import tech.guimy.couchkv.MVCCSnapshot;
import tech.guimy.couchkv.TransactionIsolationLevel;
import tech.guimy.couchkv.TxStatus;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a transaction in the KV store.
 * Provides ACID guarantees with write-ahead logging and MVCC snapshots.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Transaction<K extends Serializable & Comparable<K>, V extends Serializable> {
    
    private final long txId;
    private final KVStore<K, V> store;
    private final TransactionIsolationLevel isolationLevel;
    private final MVCCSnapshot snapshot;
    private final long snapshotSequence;
    private TxStatus status;
    private final Map<K, V> readSet;
    private final Map<K, V> writeSet;

    Transaction(long txId, KVStore<K, V> store) {
        this(txId, store, TransactionIsolationLevel.REPEATABLE_READ);
    }

    Transaction(long txId, KVStore<K, V> store, TransactionIsolationLevel isolationLevel) {
        this.txId = txId;
        this.store = store;
        this.isolationLevel = isolationLevel;
        this.status = TxStatus.ACTIVE;
        this.readSet = new HashMap<>();
        this.writeSet = new HashMap<>();
        this.snapshot = store.createSnapshot();
        this.snapshotSequence = store.getSequenceNumber();
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
     * @return the transaction isolation level
     */
    public TransactionIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }
    
    /**
     * @return the MVCC snapshot for this transaction
     */
    public MVCCSnapshot getSnapshot() {
        return snapshot;
    }
    
    /**
     * @return the sequence number at transaction start
     */
    public long getSnapshotSequence() {
        return snapshotSequence;
    }

    /**
     * Gets a value within this transaction.
     * Behavior depends on isolation level.
     */
    public V get(K key) {
        checkActive();
        
        if (writeSet.containsKey(key)) {
            return writeSet.get(key);
        }
        
        if (isolationLevel == TransactionIsolationLevel.READ_UNCOMMITTED) {
            V current = store.get(key);
            readSet.put(key, current);
            return current;
        }
        
        if (isolationLevel == TransactionIsolationLevel.READ_COMMITTED) {
            V current = store.get(key);
            readSet.put(key, current);
            return current;
        }
        
        if (readSet.containsKey(key)) {
            return readSet.get(key);
        }
        
        V value = store.getAtSequence(key, snapshotSequence);
        readSet.put(key, value);
        return value;
    }

    /**
     * Sets a value within this transaction
     */
    public void put(K key, V value) {
        checkActive();
        store.putInternal(this, key, value);
        writeSet.put(key, value);
        readSet.put(key, value);
    }

    /**
     * Deletes a key within this transaction
     */
    public void delete(K key) {
        checkActive();
        store.deleteInternal(this, key);
        writeSet.put(key, null);
        readSet.put(key, null);
    }

    /**
     * Commits this transaction
     */
    public void commit() throws IOException {
        checkActive();
        
        if (isolationLevel == TransactionIsolationLevel.SERIALIZABLE) {
            for (Map.Entry<K, V> entry : readSet.entrySet()) {
                V currentValue = store.get(entry.getKey());
                if (!Objects.equals(currentValue, entry.getValue())) {
                    throw new IOException("Serialization conflict detected for key: " + entry.getKey());
                }
            }
        }
        
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
    
    Map<K, V> getWriteSet() {
        return writeSet;
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
