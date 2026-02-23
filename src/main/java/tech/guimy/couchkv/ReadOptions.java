package tech.guimy.couchkv;

/**
 * Read options for CouchKV operations.
 * 
 * Similar to RocksDB's ReadOptions class.
 */
public class ReadOptions {

    private boolean fillCache = true;
    private boolean verifyChecksums = true;

    /**
     * Creates default read options.
     */
    public ReadOptions() {
    }

    /**
     * Sets whether to fill the cache with read data.
     * 
     * @param fillCache true to fill cache
     * @return this ReadOptions instance
     */
    public ReadOptions setFillCache(boolean fillCache) {
        this.fillCache = fillCache;
        return this;
    }

    /**
     * Checks if cache filling is enabled.
     * 
     * @return true if cache filling is enabled
     */
    public boolean isFillCache() {
        return fillCache;
    }

    /**
     * Sets whether to verify checksums during reads.
     * 
     * @param verifyChecksums true to verify checksums
     * @return this ReadOptions instance
     */
    public ReadOptions setVerifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    /**
     * Checks if checksum verification is enabled.
     * 
     * @return true if checksum verification is enabled
     */
    public boolean isVerifyChecksums() {
        return verifyChecksums;
    }

    /**
     * Returns default read options.
     * 
     * @return default read options
     */
    public static ReadOptions defaultReadOptions() {
        return new ReadOptions();
    }
}
