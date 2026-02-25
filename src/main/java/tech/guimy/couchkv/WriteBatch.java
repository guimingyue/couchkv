package tech.guimy.couchkv;

import tech.guimy.couchkv.core.KVStore;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Write batch for atomic batch operations.
 * 
 * All operations in a batch are applied atomically - either all succeed
 * or none are applied. Uses a rollback mechanism on failure.
 */
public class WriteBatch implements AutoCloseable {

    private final List<Operation> operations = new ArrayList<>();
    private final List<Operation> appliedOperations = new ArrayList<>();

    private enum OpType {
        PUT,
        DELETE
    }

    private static class Operation {
        final OpType type;
        final String key;
        final byte[] value;
        final byte[] previousValue;

        Operation(OpType type, String key, byte[] value, byte[] previousValue) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.previousValue = previousValue;
        }
    }

    public WriteBatch() {
    }

    public WriteBatch put(byte[] key, byte[] value) {
        operations.add(new Operation(OpType.PUT, bytesToString(key), value, null));
        return this;
    }

    public WriteBatch put(String key, String value) {
        operations.add(new Operation(OpType.PUT, key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8), null));
        return this;
    }

    public WriteBatch delete(byte[] key) {
        operations.add(new Operation(OpType.DELETE, bytesToString(key), null, null));
        return this;
    }

    public WriteBatch delete(String key) {
        operations.add(new Operation(OpType.DELETE, key, null, null));
        return this;
    }

    public void clear() {
        operations.clear();
        appliedOperations.clear();
    }

    @Override
    public void close() {
        clear();
    }

    public int count() {
        return operations.size();
    }

    /**
     * Executes all operations in the batch atomically.
     * If any operation fails, all previously applied operations are rolled back.
     * 
     * @param store the KVStore to execute on
     * @throws IOException if execution fails
     */
    @SuppressWarnings("unchecked")
    void execute(KVStore<String, byte[]> store) throws IOException {
        appliedOperations.clear();
        
        java.util.Set<String> deletedKeys = new java.util.HashSet<>();
        java.util.Map<String, byte[]> previousValues = new java.util.HashMap<>();
        
        try {
            for (Operation op : operations) {
                byte[] prevValue = store.get(op.key);
                previousValues.put(op.key, prevValue);
                
                if (op.type == OpType.PUT) {
                    store.put(op.key, op.value);
                } else if (op.type == OpType.DELETE) {
                    store.delete(op.key);
                    deletedKeys.add(op.key);
                }
                
                Operation applied = new Operation(op.type, op.key, op.value, prevValue);
                appliedOperations.add(applied);
            }
        } catch (IOException e) {
            rollback(store);
            throw e;
        } catch (Exception e) {
            rollback(store);
            throw new IOException("Batch execution failed, rolled back", e);
        }
    }
    
    /**
     * Rolls back all applied operations.
     */
    @SuppressWarnings("unchecked")
    private void rollback(KVStore<String, byte[]> store) throws IOException {
        for (int i = appliedOperations.size() - 1; i >= 0; i--) {
            Operation op = appliedOperations.get(i);
            try {
                if (op.previousValue != null) {
                    store.put(op.key, op.previousValue);
                } else {
                    store.delete(op.key);
                }
            } catch (Exception e) {
                throw new IOException("Rollback failed for key: " + op.key, e);
            }
        }
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
