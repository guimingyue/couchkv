package tech.guimy.couchkv;

/**
 * Database options for CouchKV.
 * 
 * Similar to RocksDB's Options class.
 */
public class Options {

    private double fragmentationThreshold = 0.3;
    private boolean autoCompact = false;
    private boolean createIfMissing = true;
    private boolean errorIfExists = false;
    private TransactionIsolationLevel isolationLevel = TransactionIsolationLevel.REPEATABLE_READ;

    /**
     * Creates default options.
     */
    public Options() {
    }

    /**
     * Sets the fragmentation threshold for auto-compaction.
     * When fragmentation exceeds this threshold, compaction is triggered.
     * 
     * @param threshold the threshold (0.0 to 1.0)
     * @return this Options instance
     */
    public Options setFragmentationThreshold(double threshold) {
        if (threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("Threshold must be between 0.0 and 1.0");
        }
        this.fragmentationThreshold = threshold;
        return this;
    }

    /**
     * Gets the fragmentation threshold.
     * 
     * @return the threshold
     */
    public double getFragmentationThreshold() {
        return fragmentationThreshold;
    }

    /**
     * Enables or disables automatic compaction.
     * 
     * @param autoCompact true to enable auto compaction
     * @return this Options instance
     */
    public Options setAutoCompact(boolean autoCompact) {
        this.autoCompact = autoCompact;
        return this;
    }

    /**
     * Checks if auto compaction is enabled.
     * 
     * @return true if enabled
     */
    public boolean isAutoCompact() {
        return autoCompact;
    }

    /**
     * Sets whether to create the database if it doesn't exist.
     * 
     * @param createIfMissing true to create if missing
     * @return this Options instance
     */
    public Options setCreateIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
        return this;
    }

    /**
     * Checks if database should be created if missing.
     * 
     * @return true if should create
     */
    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    /**
     * Sets whether to raise an error if the database already exists.
     * 
     * @param errorIfExists true to raise error if exists
     * @return this Options instance
     */
    public Options setErrorIfExists(boolean errorIfExists) {
        this.errorIfExists = errorIfExists;
        return this;
    }

    /**
     * Checks if error should be raised if database exists.
     * 
     * @return true if should error
     */
    public boolean isErrorIfExists() {
        return errorIfExists;
    }

    /**
     * Sets the transaction isolation level.
     * 
     * @param isolationLevel the isolation level
     * @return this Options instance
     */
    public Options setIsolationLevel(TransactionIsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }

    /**
     * Gets the transaction isolation level.
     * 
     * @return the isolation level
     */
    public TransactionIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }
}
