package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Leaf node in the B+Tree.
 */
final class BTreeLeafNode<K extends Serializable & Comparable<K>, V extends Serializable>
    extends BTreeNode<K, V> {

    final List<V> values = new ArrayList<>();
    BTreeNode<K, V> nextLeaf;

    BTreeLeafNode() {}

    @Override
    V get(K key) {
        int idx = findKeyIndex(key);
        return (idx >= 0 && idx < values.size()) ? values.get(idx) : null;
    }

    @Override
    SplitResult<K, V> insert(K key, V value) {
        int idx = findKeyIndex(key);
        if (idx >= 0) {
            // Update existing
            values.set(idx, value);
            return null;
        }

        // Insert new key-value
        int insertPos = -(idx) - 1;
        keys.add(insertPos, key);
        values.add(insertPos, value);

        if (isFull()) {
            return split();
        }
        return null;
    }

    @Override
    void delete(K key) {
        int idx = findKeyIndex(key);
        if (idx >= 0 && idx < values.size()) {
            values.set(idx, null);  // Tombstone
        }
    }

    @Override
    List<Entry<K, V>> range(K start, K end) {
        List<Entry<K, V>> results = new ArrayList<>();

        int startPos = 0;
        if (start != null) {
            int idx = findKeyIndex(start);
            startPos = (idx >= 0) ? idx : Math.max(0, -(idx) - 1);
        }

        for (int i = startPos; i < keys.size(); i++) {
            K key = keys.get(i);

            if (end != null && key.compareTo(end) > 0) {
                break;
            }

            V value = values.get(i);
            if (value != null && (start == null || key.compareTo(start) >= 0)) {
                results.add(new Entry<>(key, value));
            }
        }

        return results;
    }

    @Override
    int size() {
        int count = 0;
        for (V v : values) {
            if (v != null) count++;
        }
        return count;
    }

    /**
     * Splits this node and returns the split result
     */
    private SplitResult<K, V> split() {
        int mid = keys.size() / 2;

        BTreeLeafNode<K, V> right = new BTreeLeafNode<>();

        // Move upper half to right node
        for (int i = keys.size() - 1; i >= mid; i--) {
            right.keys.add(0, keys.remove(i));
            right.values.add(0, values.remove(i));
        }

        // Link leaves
        right.nextLeaf = this.nextLeaf;
        this.nextLeaf = right;

        return new SplitResult<>(right.keys.get(0), right);
    }
}
