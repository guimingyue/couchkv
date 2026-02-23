package tech.guimy.couchkv;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator for scanning key-value entries.
 * 
 * Similar to RocksDB's Iterator class.
 */
public class KVIterator implements AutoCloseable {

    private final Iterator<SimpleEntry> iterator;
    private SimpleEntry current;
    private boolean valid;

    /**
     * Simple entry for iteration.
     */
    public static class SimpleEntry {
        private final byte[] key;
        private final byte[] value;

        public SimpleEntry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }

    /**
     * Creates a new iterator from a list of entries.
     * 
     * @param entries the entries to iterate over
     */
    public KVIterator(List<tech.guimy.couchkv.Entry<String, byte[]>> entries) {
        this.iterator = entries.stream()
            .map(e -> new SimpleEntry(e.key().getBytes(java.nio.charset.StandardCharsets.UTF_8), e.value()))
            .iterator();
        this.valid = iterator.hasNext();
        if (valid) {
            this.current = iterator.next();
        }
    }

    /**
     * Checks if the iterator is positioned at a valid entry.
     * 
     * @return true if valid
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * Moves to the next entry.
     */
    public void next() {
        if (iterator.hasNext()) {
            current = iterator.next();
            valid = true;
        } else {
            valid = false;
            current = null;
        }
    }

    /**
     * Moves to the previous entry.
     * Note: This is a simplified implementation. For full backward iteration,
     * consider using a different approach.
     * 
     * @throws UnsupportedOperationException if not supported
     */
    public void prev() {
        throw new UnsupportedOperationException("Backward iteration not supported in this version");
    }

    /**
     * Seeks to the first entry with key >= target.
     * 
     * @param target the target key
     */
    public void seek(byte[] target) {
        throw new UnsupportedOperationException("Seek not supported in this version");
    }

    /**
     * Seeks to the first entry.
     */
    public void seekToFirst() {
        throw new UnsupportedOperationException("SeekToFirst not supported in this version");
    }

    /**
     * Seeks to the last entry.
     */
    public void seekToLast() {
        throw new UnsupportedOperationException("SeekToLast not supported in this version");
    }

    /**
     * Gets the current key.
     * 
     * @return the current key, or null if not valid
     */
    public byte[] key() {
        if (!valid || current == null) {
            return null;
        }
        return current.getKey();
    }

    /**
     * Gets the current value.
     * 
     * @return the current value, or null if not valid
     */
    public byte[] value() {
        if (!valid || current == null) {
            return null;
        }
        return current.getValue();
    }

    /**
     * Gets the current entry status.
     * 
     * @return the status (always OK in this implementation)
     */
    public Status status() {
        return Status.OK;
    }

    @Override
    public void close() {
        valid = false;
        current = null;
    }

    /**
     * Status codes.
     */
    public enum Status {
        OK,
        CORRUPTION,
        NOT_FOUND,
        IO_ERROR
    }
}
