package tech.guimy.couchkv;

import tech.guimy.couchkv.core.KVStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Write batch for atomic batch operations.
 * 
 * Similar to RocksDB's WriteBatch class.
 */
public class WriteBatch implements AutoCloseable {

    private final List<Operation> operations = new ArrayList<>();

    /**
     * Operation types.
     */
    private enum OpType {
        PUT,
        DELETE
    }

    /**
     * Internal operation class.
     */
    private static class Operation {
        final OpType type;
        final String key;
        final byte[] value;

        Operation(OpType type, String key, byte[] value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Creates an empty write batch.
     */
    public WriteBatch() {
    }

    /**
     * Adds a put operation to the batch.
     * 
     * @param key the key
     * @param value the value
     * @return this WriteBatch instance
     */
    public WriteBatch put(byte[] key, byte[] value) {
        operations.add(new Operation(OpType.PUT, bytesToString(key), value));
        return this;
    }

    /**
     * Adds a put operation to the batch.
     * 
     * @param key the key
     * @param value the value
     * @return this WriteBatch instance
     */
    public WriteBatch put(String key, String value) {
        operations.add(new Operation(OpType.PUT, key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return this;
    }

    /**
     * Adds a delete operation to the batch.
     * 
     * @param key the key
     * @return this WriteBatch instance
     */
    public WriteBatch delete(byte[] key) {
        operations.add(new Operation(OpType.DELETE, bytesToString(key), null));
        return this;
    }

    /**
     * Adds a delete operation to the batch.
     * 
     * @param key the key
     * @return this WriteBatch instance
     */
    public WriteBatch delete(String key) {
        operations.add(new Operation(OpType.DELETE, key, null));
        return this;
    }

    /**
     * Clears all operations in the batch.
     */
    public void clear() {
        operations.clear();
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Gets the number of operations in the batch.
     * 
     * @return the number of operations
     */
    public int count() {
        return operations.size();
    }

    /**
     * Executes all operations in the batch.
     * 
     * @param store the KVStore to execute on
     * @throws IOException if execution fails
     */
    void execute(KVStore<String, byte[]> store) throws IOException {
        for (Operation op : operations) {
            if (op.type == OpType.PUT) {
                store.put(op.key, op.value);
            } else if (op.type == OpType.DELETE) {
                store.delete(op.key);
            }
        }
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
