package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for append-only B+Tree nodes.
 * Each node is immutable once written - updates create new nodes.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
abstract class BTreeNode<K extends Serializable & Comparable<K>, V extends Serializable> {
    
    protected final List<K> keys = new ArrayList<>();
    protected long fileOffset = -1;  // Offset in the data file
    protected long sequenceNumber;   // Sequence number for MVCC

    /**
     * Gets the value for a key
     */
    abstract V get(K key);
    
    /**
     * Inserts a key-value pair (creates new node for append-only)
     * @return SplitResult if split occurred, null otherwise
     */
    abstract SplitResult<K, V> insert(K key, V value);
    
    /**
     * Deletes a key (creates new node for append-only)
     */
    abstract void delete(K key);
    
    /**
     * Gets all entries in range [start, end]
     */
    abstract List<Entry<K, V>> range(K start, K end);
    
    /**
     * @return the number of entries in this subtree
     */
    abstract int size();
    
    /**
     * @return the number of keys in this node
     */
    int getKeyCount() {
        return keys.size();
    }
    
    /**
     * @return true if this node is full
     */
    boolean isFull() {
        return keys.size() >= ORDER - 1;
    }
    
    /**
     * @return true if this node is underflowed
     */
    boolean isUnderflowed() {
        return keys.size() < (ORDER / 2) - 1;
    }
    
    /**
     * Finds the index of a key using binary search
     */
    protected int findKeyIndex(K key) {
        int low = 0, high = keys.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = key.compareTo(keys.get(mid));
            if (cmp < 0) high = mid - 1;
            else if (cmp > 0) low = mid + 1;
            else return mid;
        }
        return -(low) - 1;
    }
    
    protected static final int ORDER = 32;
    
    /**
     * Result of a node split
     */
    static class SplitResult<K extends Serializable & Comparable<K>, V extends Serializable> {
        final K separatorKey;
        final BTreeNode<K, V> rightNode;
        
        SplitResult(K separatorKey, BTreeNode<K, V> rightNode) {
            this.separatorKey = separatorKey;
            this.rightNode = rightNode;
        }
    }
}
