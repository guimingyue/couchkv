package tech.guimy.couchkv;

import tech.guimy.couchkv.core.Compactor;
import tech.guimy.couchkv.core.KVStore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CouchKV - A simple, lightweight key-value store with B+Tree and ACID transactions.
 * 
 * This is the main entry point for CouchKV operations, similar to RocksDB's API.
 * 
 * @author CouchKV Team
 * @version 1.0.0
 */
public class CouchKV implements Closeable {

    private final KVStore<String, byte[]> store;
    private final Path dbPath;
    private volatile boolean closed;

    /**
     * Opens or creates a CouchKV database at the specified path.
     * 
     * @param path the database file path
     * @return a new CouchKV instance
     * @throws IOException if the database cannot be opened
     */
    public static CouchKV open(Path path) throws IOException {
        return new CouchKV(path);
    }

    /**
     * Opens or creates a CouchKV database with custom options.
     * 
     * @param path the database file path
     * @param options database options
     * @return a new CouchKV instance
     * @throws IOException if the database cannot be opened
     */
    public static CouchKV open(Path path, Options options) throws IOException {
        return new CouchKV(path, options);
    }

    /**
     * Creates a new CouchKV database, deleting any existing data.
     * 
     * @param path the database file path
     * @return a new CouchKV instance
     * @throws IOException if the database cannot be created
     */
    public static CouchKV create(Path path) throws IOException {
        return create(path, new Options());
    }

    /**
     * Creates a new CouchKV database with custom options, deleting any existing data.
     * 
     * @param path the database file path
     * @param options database options
     * @return a new CouchKV instance
     * @throws IOException if the database cannot be created
     */
    public static CouchKV create(Path path, Options options) throws IOException {
        java.nio.file.Files.deleteIfExists(path);
        java.nio.file.Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".wal"));
        return new CouchKV(path, options);
    }

    private CouchKV(Path path) throws IOException {
        this(path, new Options());
    }

    @SuppressWarnings("unchecked")
    private CouchKV(Path path, Options options) throws IOException {
        this.dbPath = path;
        this.store = new KVStore<>(path, options.getFragmentationThreshold(), options.isAutoCompact());
        this.closed = false;
    }

    // ==================== Basic Operations ====================

    /**
     * Stores a key-value pair.
     * 
     * @param key the key
     * @param value the value
     * @throws IOException if the write fails
     */
    public void put(byte[] key, byte[] value) throws IOException {
        checkOpen();
        store.put(bytesToString(key), value);
    }

    /**
     * Stores a key-value pair with custom write options.
     * 
     * @param key the key
     * @param value the value
     * @param writeOptions write options
     * @throws IOException if the write fails
     */
    public void put(byte[] key, byte[] value, WriteOptions writeOptions) throws IOException {
        checkOpen();
        if (writeOptions != null && writeOptions.isSync()) {
            store.put(bytesToString(key), value);
            store.flush();
        } else {
            store.put(bytesToString(key), value);
        }
    }

    /**
     * Stores a string key-value pair.
     * 
     * @param key the key
     * @param value the value
     * @throws IOException if the write fails
     */
    public void put(String key, String value) throws IOException {
        checkOpen();
        store.put(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Stores a key-value pair.
     * 
     * @param key the key
     * @param value the value
     * @throws IOException if the write fails
     */
    public void put(byte[] key, String value) throws IOException {
        checkOpen();
        store.put(bytesToString(key), value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Retrieves a value by key.
     * 
     * @param key the key
     * @return the value, or null if not found
     * @throws IOException if the read fails
     */
    public byte[] get(byte[] key) throws IOException {
        checkOpen();
        return store.get(bytesToString(key));
    }

    /**
     * Retrieves a value by key with custom read options.
     * 
     * @param key the key
     * @param readOptions read options
     * @return the value, or null if not found
     * @throws IOException if the read fails
     */
    public byte[] get(byte[] key, ReadOptions readOptions) throws IOException {
        checkOpen();
        return store.get(bytesToString(key));
    }

    /**
     * Retrieves a value by string key.
     * 
     * @param key the key
     * @return the value as string, or null if not found
     * @throws IOException if the read fails
     */
    @SuppressWarnings("unchecked")
    public String getString(String key) throws IOException {
        checkOpen();
        Object value = store.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    /**
     * Retrieves multiple values by keys.
     * 
     * @param keys the keys
     * @return a map of key-value pairs
     * @throws IOException if the read fails
     */
    public Map<byte[], byte[]> multiGet(List<byte[]> keys) throws IOException {
        checkOpen();
        Map<byte[], byte[]> results = new ConcurrentHashMap<>();
        for (byte[] key : keys) {
            byte[] value = get(key);
            if (value != null) {
                results.put(key, value);
            }
        }
        return results;
    }

    /**
     * Deletes a key-value pair.
     * 
     * @param key the key
     * @throws IOException if the delete fails
     */
    public void delete(byte[] key) throws IOException {
        checkOpen();
        store.delete(bytesToString(key));
    }

    /**
     * Deletes a key-value pair with custom write options.
     * 
     * @param key the key
     * @param writeOptions write options
     * @throws IOException if the delete fails
     */
    public void delete(byte[] key, WriteOptions writeOptions) throws IOException {
        checkOpen();
        store.delete(bytesToString(key));
        if (writeOptions != null && writeOptions.isSync()) {
            store.flush();
        }
    }

    /**
     * Deletes a string key.
     * 
     * @param key the key
     * @throws IOException if the delete fails
     */
    public void delete(String key) throws IOException {
        delete(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    // ==================== Batch Operations ====================

    /**
     * Creates a new write batch.
     * 
     * @return a new WriteBatch instance
     */
    public WriteBatch createWriteBatch() {
        return new WriteBatch();
    }

    /**
     * Writes a batch of operations.
     * 
     * @param batch the write batch
     * @throws IOException if the write fails
     */
    public void write(WriteBatch batch) throws IOException {
        checkOpen();
        batch.execute(store);
    }

    /**
     * Writes a batch of operations with custom write options.
     * 
     * @param batch the write batch
     * @param writeOptions write options
     * @throws IOException if the write fails
     */
    public void write(WriteBatch batch, WriteOptions writeOptions) throws IOException {
        checkOpen();
        batch.execute(store);
        if (writeOptions != null && writeOptions.isSync()) {
            store.flush();
        }
    }

    // ==================== Iteration ====================

    /**
     * Creates a new iterator for scanning all entries.
     * 
     * @return a new KVIterator instance
     * @throws IOException if the iterator cannot be created
     */
    public KVIterator newIterator() throws IOException {
        checkOpen();
        return new KVIterator(store.scan());
    }

    /**
     * Creates a new iterator for a range of entries.
     * 
     * @param startKey the start key (inclusive)
     * @param endKey the end key (inclusive)
     * @return a new KVIterator instance
     * @throws IOException if the iterator cannot be created
     */
    public KVIterator newIterator(byte[] startKey, byte[] endKey) throws IOException {
        checkOpen();
        return new KVIterator(store.range(bytesToString(startKey), bytesToString(endKey)));
    }

    // ==================== Compaction ====================

    /**
     * Manually triggers compaction for the entire key range.
     * 
     * @throws IOException if compaction fails
     */
    public void compactRange() throws IOException {
        checkOpen();
        store.compactor().compact();
    }

    /**
     * Manually triggers compaction for a specific key range.
     * 
     * @param beginKey the begin key
     * @param endKey the end key
     * @throws IOException if compaction fails
     */
    public void compactRange(byte[] beginKey, byte[] endKey) throws IOException {
        checkOpen();
        store.compactor().compact();
    }

    /**
     * Gets compaction statistics.
     * 
     * @return compaction statistics, or null if no compaction has run
     */
    public CompactionStats getCompactionStats() {
        return store.compactor().getLastStats();
    }

    // ==================== Properties ====================

    /**
     * Gets a property value.
     * 
     * @param property the property name
     * @return the property value, or null if not found
     */
    public String getProperty(String property) {
        switch (property) {
            case "couchkv.num-keys":
                return String.valueOf(store.size());
            case "couchkv.file-size":
                return String.valueOf(store.getFileSize());
            case "couchkv.memtable-size":
                return "0";  // Internal implementation detail
            default:
                return null;
        }
    }

    /**
     * Gets the approximate number of keys in the database.
     * 
     * @return the number of keys
     */
    public long getApproximateNumKeys() {
        return store.size();
    }

    /**
     * Gets the current file size in bytes.
     * 
     * @return the file size
     */
    public long getFileSize() {
        return store.getFileSize();
    }

    // ==================== Utilities ====================

    /**
     * Flushes all pending writes to disk.
     * 
     * @throws IOException if the flush fails
     */
    public void flush() throws IOException {
        checkOpen();
        store.flush();
    }

    /**
     * Checks if the database is open.
     * 
     * @return true if open, false otherwise
     */
    public boolean isOpen() {
        return !closed;
    }

    /**
     * Closes the database.
     * 
     * @throws IOException if the close fails
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            store.close();
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
    }

    private String bytesToString(byte[] bytes) {
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    // ==================== Property Names ====================

    /**
     * Property name for number of keys.
     */
    public static final String PROPERTY_NUM_KEYS = "couchkv.num-keys";

    /**
     * Property name for file size.
     */
    public static final String PROPERTY_FILE_SIZE = "couchkv.file-size";
}
