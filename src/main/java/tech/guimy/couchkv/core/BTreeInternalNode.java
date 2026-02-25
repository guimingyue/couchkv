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
 * Internal (non-leaf) node in the append-only B+Tree.
 * Contains keys and child pointers.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
final class BTreeInternalNode<K extends Serializable & Comparable<K>, V extends Serializable> 
    extends BTreeNode<K, V> {

    final List<BTreeNode<K, V>> children = new ArrayList<>();

    BTreeInternalNode() {}

    void addChild(BTreeNode<K, V> child) {
        children.add(child);
    }

    void addKey(K key) {
        keys.add(key);
    }

    @Override
    V get(K key) {
        int childIdx = findChildIndex(key);
        return children.get(childIdx).get(key);
    }

    @Override
    SplitResult<K, V> insert(K key, V value) {
        int childIdx = findChildIndex(key);
        SplitResult<K, V> splitResult = children.get(childIdx).insert(key, value);
        
        if (splitResult != null) {
            children.add(childIdx + 1, splitResult.rightNode);
            keys.add(childIdx, splitResult.separatorKey);
        }
        
        if (isFull()) {
            return split();
        }
        return null;
    }

    @Override
    void delete(K key) {
        int childIdx = findChildIndex(key);
        children.get(childIdx).delete(key);
    }

    @Override
    List<Entry<K, V>> range(K start, K end) {
        List<Entry<K, V>> results = new ArrayList<>();
        
        int startPos = (start == null) ? 0 : findChildIndex(start);
        
        for (int i = startPos; i < children.size(); i++) {
            K childStartKey = (i == startPos) ? start : null;
            List<Entry<K, V>> childResults = children.get(i).range(childStartKey, end);
            
            results.addAll(childResults);
            
            if (end != null && !childResults.isEmpty()) {
                Entry<K, V> lastEntry = childResults.get(childResults.size() - 1);
                if (lastEntry.key().compareTo(end) >= 0) {
                    break;
                }
            }
        }
        
        return results;
    }

    @Override
    int size() {
        int total = 0;
        for (BTreeNode<K, V> child : children) {
            total += child.size();
        }
        return total;
    }

    private int findChildIndex(K key) {
        for (int i = 0; i < keys.size(); i++) {
            if (key.compareTo(keys.get(i)) < 0) {
                return i;
            }
        }
        return children.size() - 1;
    }

    /**
     * Splits this node and returns the split result
     */
    SplitResult<K, V> split() {
        int mid = keys.size() / 2;
        
        BTreeInternalNode<K, V> right = new BTreeInternalNode<>();
        
        // The middle key goes up as separator (don't add to right node)
        K separatorKey = keys.get(mid);
        
        // Move keys after mid to right node
        for (int i = keys.size() - 1; i > mid; i--) {
            right.keys.add(0, keys.remove(i));
        }
        
        // Move children after mid to right node
        for (int i = children.size() - 1; i > mid; i--) {
            right.children.add(0, children.remove(i));
        }
        
        return new SplitResult<>(separatorKey, right);
    }

    @Override
    byte getNodeType() {
        return NODE_TYPE_INTERNAL;
    }

    @Override
    ByteBuffer serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeByte(NODE_TYPE_INTERNAL);
        dos.writeInt(keys.size());
        dos.writeInt(children.size());
        
        for (K key : keys) {
            writeKey(dos, key);
        }
        
        for (BTreeNode<K, V> child : children) {
            dos.writeLong(child.fileOffset);
        }
        
        dos.flush();
        byte[] data = baos.toByteArray();
        
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length + 4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(data.length);
        buffer.put(data);
        
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        buffer.putInt((int) crc32.getValue());
        
        buffer.flip();
        return buffer;
    }
    
    static <K extends Serializable & Comparable<K>, V extends Serializable> 
            BTreeInternalNode<K, V> deserialize(ByteBuffer buffer,
            java.util.function.Function<Byte, K> keyReader) throws IOException {
        BTreeInternalNode<K, V> node = new BTreeInternalNode<>();
        
        int keyCount = buffer.getInt();
        int childCount = buffer.getInt();
        
        for (int i = 0; i < keyCount; i++) {
            byte keyType = buffer.get();
            K key = keyReader.apply(keyType);
            node.keys.add(key);
        }
        
        return node;
    }
    
    void loadChildOffsets(ByteBuffer buffer, int childCount) {
        children.clear();
        for (int i = 0; i < childCount; i++) {
            BTreePlaceholderNode<K, V> placeholder = new BTreePlaceholderNode<>();
            placeholder.fileOffset = buffer.getLong();
            children.add(placeholder);
        }
    }
    
    private static class BTreePlaceholderNode<K extends Serializable & Comparable<K>, V extends Serializable> 
            extends BTreeNode<K, V> {
        @Override V get(K key) { throw new UnsupportedOperationException(); }
        @Override SplitResult<K, V> insert(K key, V value) { throw new UnsupportedOperationException(); }
        @Override void delete(K key) { throw new UnsupportedOperationException(); }
        @Override List<Entry<K, V>> range(K start, K end) { throw new UnsupportedOperationException(); }
        @Override int size() { return 0; }
        @Override ByteBuffer serialize() { throw new UnsupportedOperationException(); }
        @Override byte getNodeType() { return 0; }
    }
}
