package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
}
