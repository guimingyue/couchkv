package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * A key-value entry in the KV store.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public record Entry<K extends Serializable & Comparable<K>, V extends Serializable>(
    K key, 
    V value
) implements Serializable {

    /**
     * Comparator for comparing entries by key
     */
    public static final Comparator<Entry<?, ?>> KEY_COMPARATOR = 
        Comparator.comparing(Entry::key);

    @Override
    public String toString() {
        return key + " = " + value;
    }
}
