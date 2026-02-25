package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a point-in-time snapshot of the database for MVCC.
 * 
 * Similar to CouchDB's MVCC model, each snapshot captures:
 * - A sequence number at the time of creation
 * - A copy of the memtable state
 * - References to committed transactions before this snapshot
 */
public final class MVCCSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final long sequenceNumber;
    private final long timestamp;
    private final Map<Object, Object> memtableSnapshot;
    private final String snapshotId;
    
    public MVCCSnapshot(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = System.currentTimeMillis();
        this.memtableSnapshot = new HashMap<>();
        this.snapshotId = generateSnapshotId();
    }
    
    public MVCCSnapshot(long sequenceNumber, Map<?, ?> memtableSnapshot) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = System.currentTimeMillis();
        this.memtableSnapshot = new HashMap<>(memtableSnapshot);
        this.snapshotId = generateSnapshotId();
    }
    
    /**
     * Creates a deep copy snapshot of the memtable.
     */
    public static <K, V> MVCCSnapshot create(long sequenceNumber, Map<K, V> memtable) {
        return new MVCCSnapshot(sequenceNumber, memtable);
    }
    
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getSnapshotId() {
        return snapshotId;
    }
    
    /**
     * Gets a value from the memtable snapshot.
     */
    @SuppressWarnings("unchecked")
    public <V> V get(Object key) {
        return (V) memtableSnapshot.get(key);
    }
    
    /**
     * Gets a value from the memtable snapshot using String key.
     */
    @SuppressWarnings("unchecked")
    public <V> V get(String key) {
        return (V) memtableSnapshot.get(key);
    }
    
    /**
     * Checks if a key exists in the snapshot.
     */
    public boolean containsKey(Object key) {
        return memtableSnapshot.containsKey(key);
    }
    
    /**
     * Returns an unmodifiable view of the memtable snapshot.
     */
    public Map<Object, Object> getMemtableSnapshot() {
        return Collections.unmodifiableMap(memtableSnapshot);
    }
    
    private static String generateSnapshotId() {
        return String.format("snap-%d-%08x", 
            System.currentTimeMillis(), 
            (int)(Math.random() * Integer.MAX_VALUE));
    }
    
    @Override
    public String toString() {
        return "MVCCSnapshot{" +
                "seq=" + sequenceNumber +
                ", id='" + snapshotId + '\'' +
                ", entries=" + memtableSnapshot.size() +
                '}';
    }
}