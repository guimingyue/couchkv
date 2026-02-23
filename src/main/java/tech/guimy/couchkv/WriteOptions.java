package tech.guimy.couchkv;

/**
 * Write options for CouchKV operations.
 * 
 * Similar to RocksDB's WriteOptions class.
 */
public class WriteOptions {

    private boolean sync = false;

    /**
     * Creates default write options.
     */
    public WriteOptions() {
    }

    /**
     * Sets whether to sync writes to disk before returning.
     * When enabled, provides stronger durability guarantees.
     * 
     * @param sync true to sync writes
     * @return this WriteOptions instance
     */
    public WriteOptions setSync(boolean sync) {
        this.sync = sync;
        return this;
    }

    /**
     * Checks if sync is enabled.
     * 
     * @return true if sync is enabled
     */
    public boolean isSync() {
        return sync;
    }

    /**
     * Returns write options with sync enabled.
     * 
     * @return write options with sync enabled
     */
    public static WriteOptions defaultWriteOptions() {
        return new WriteOptions();
    }

    /**
     * Returns write options with sync enabled for strong durability.
     * 
     * @return write options with sync enabled
     */
    public static WriteOptions syncWriteOptions() {
        return new WriteOptions().setSync(true);
    }
}
