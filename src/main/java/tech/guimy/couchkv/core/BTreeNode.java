package tech.guimy.couchkv.core;

import tech.guimy.couchkv.Entry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Base class for append-only B+Tree nodes.
 * Each node is immutable once written - updates create new nodes.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
abstract class BTreeNode<K extends Serializable & Comparable<K>, V extends Serializable> {
    
    protected static final byte NODE_TYPE_LEAF = 1;
    protected static final byte NODE_TYPE_INTERNAL = 2;
    
    protected final List<K> keys = new ArrayList<>();
    protected long fileOffset = -1;
    protected long sequenceNumber;

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
    
    /**
     * Serializes this node to a ByteBuffer for persistent storage.
     */
    abstract ByteBuffer serialize() throws IOException;
    
    /**
     * Gets the node type for serialization.
     */
    abstract byte getNodeType();
    
    /**
     * Writes a key to the output stream.
     */
    protected void writeKey(DataOutputStream dos, K key) throws IOException {
        if (key instanceof String) {
            dos.writeByte(0);
            byte[] bytes = ((String) key).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (key instanceof Integer) {
            dos.writeByte(1);
            dos.writeInt((Integer) key);
        } else if (key instanceof Long) {
            dos.writeByte(2);
            dos.writeLong((Long) key);
        } else {
            dos.writeByte(127);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(key);
            byte[] bytes = baos.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }
    
    /**
     * Writes a value to the output stream.
     */
    protected void writeValue(DataOutputStream dos, V value) throws IOException {
        if (value == null) {
            dos.writeByte(-1);
        } else if (value instanceof String) {
            dos.writeByte(0);
            byte[] bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (value instanceof Integer) {
            dos.writeByte(1);
            dos.writeInt((Integer) value);
        } else if (value instanceof Long) {
            dos.writeByte(2);
            dos.writeLong((Long) value);
        } else if (value instanceof byte[]) {
            dos.writeByte(3);
            byte[] bytes = (byte[]) (Object) value;
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else {
            dos.writeByte(127);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(value);
            byte[] bytes = baos.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }
}
