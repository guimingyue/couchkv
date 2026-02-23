package tech.guimy.couchkv.core;

import tech.guimy.couchkv.CompactionStats;
import tech.guimy.couchkv.Entry;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Compactor for the append-only B+Tree KV Store.
 *
 * In an append-only storage system:
 * - Deletes create tombstones (null values) instead of removing entries
 * - Updates create new versions of nodes
 * - Over time, the file accumulates obsolete data
 *
 * Compaction can be triggered:
 * 1. **Manually**: Call `compact()` directly
 * 2. **Automatically**: When fragmentation exceeds threshold (autoCompact enabled)
 * 3. **Scheduled**: Periodic compaction at fixed intervals
 *
 * Compaction process:
 * 1. Scan all active (non-tombstone) entries
 * 2. Build a new compacted B+Tree with only active entries
 * 3. Truncate the old file and write the new tree
 * 4. Update metadata
 *
 * This is similar to CouchDB's compaction which reclaims space
 * from deleted documents and old revisions.
 */
public class Compactor<K extends Serializable & Comparable<K>, V extends Serializable> {

    private final KVStore<K, V> store;
    private volatile boolean compacting;
    private volatile CompactionStats lastStats;
    private volatile Instant lastCompaction;

    // Fragmentation threshold (percentage of obsolete data)
    private static final double DEFAULT_COMPACTION_THRESHOLD = 0.3;
    
    // Auto compaction settings
    private final double fragmentationThreshold;
    private final boolean autoCompact;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean scheduledCompactionRunning = new AtomicBoolean(false);

    public Compactor(KVStore<K, V> store) {
        this(store, DEFAULT_COMPACTION_THRESHOLD, false);
    }

    public Compactor(KVStore<K, V> store, double fragmentationThreshold, boolean autoCompact) {
        this.store = store;
        this.fragmentationThreshold = fragmentationThreshold;
        this.autoCompact = autoCompact;
        this.compacting = false;
        this.scheduler = autoCompact ? Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "couchkv-compactor");
            t.setDaemon(true);
            return t;
        }) : null;
        
        if (autoCompact) {
            // Check every 30 seconds if compaction is needed
            scheduler.scheduleAtFixedRate(
                this::tryAutoCompact,
                30,
                30,
                TimeUnit.SECONDS
            );
        }
    }

    /**
     * Runs compaction on the store
     */
    public CompactionStats compact() throws IOException {
        if (compacting) {
            throw new IllegalStateException("Compaction already in progress");
        }

        compacting = true;
        Instant startTime = Instant.now();

        try {
            // Get current stats
            long entriesBefore = store.size();
            long sizeBefore = store.getFileSize();
            int fragmentationBefore = estimateFragmentation();

            // Scan all active entries (excludes tombstones)
            List<Entry<K, V>> activeEntries = store.scan();

            // Rebuild tree with only active entries
            store.rebuildFromEntries(activeEntries);

            // Get new stats
            long entriesAfter = store.size();
            long sizeAfter = store.getFileSize();
            int fragmentationAfter = estimateFragmentation();

            Instant endTime = Instant.now();

            lastStats = new CompactionStats(
                startTime,
                endTime,
                entriesBefore,
                entriesAfter,
                sizeBefore,
                sizeAfter,
                fragmentationBefore,
                fragmentationAfter
            );

            lastCompaction = endTime;

            return lastStats;

        } finally {
            compacting = false;
        }
    }

    /**
     * Automatically triggers compaction if fragmentation exceeds threshold.
     * Called periodically by the scheduler when autoCompact is enabled.
     */
    private void tryAutoCompact() {
        if (scheduledCompactionRunning.get()) {
            return;  // Another auto compaction is running
        }
        
        if (compacting) {
            return;  // Manual compaction in progress
        }
        
        if (needsCompaction()) {
            if (scheduledCompactionRunning.compareAndSet(false, true)) {
                try {
                    compact();
                } catch (IOException e) {
                    // Log but don't propagate - auto compaction is best effort
                    e.printStackTrace();
                } finally {
                    scheduledCompactionRunning.set(false);
                }
            }
        }
    }

    /**
     * @return true if compaction is currently running
     */
    public boolean isCompacting() {
        return compacting;
    }

    /**
     * @return statistics from the last compaction
     */
    public CompactionStats getLastStats() {
        return lastStats;
    }

    /**
     * @return the time of the last compaction
     */
    public Instant getLastCompaction() {
        return lastCompaction;
    }

    /**
     * Checks if compaction is recommended based on fragmentation
     * @param threshold fragmentation threshold (0.0 to 1.0)
     * @return true if compaction is recommended
     */
    public boolean needsCompaction(double threshold) {
        if (store.isEmpty()) {
            return false;
        }

        int fragmentation = estimateFragmentation();
        return (fragmentation / 100.0) >= threshold;
    }

    /**
     * Checks if compaction is recommended using configured threshold
     */
    public boolean needsCompaction() {
        return needsCompaction(fragmentationThreshold);
    }

    /**
     * Schedules compaction to run after a delay.
     * Useful for deferred compaction after bulk operations.
     * 
     * @param delay the delay before running compaction
     * @param unit the time unit of the delay
     */
    public void scheduleCompaction(long delay, TimeUnit unit) {
        if (scheduler == null) {
            throw new IllegalStateException("Scheduler not available (autoCompact not enabled)");
        }
        scheduler.schedule(() -> {
            try {
                compact();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, delay, unit);
    }

    /**
     * Shuts down the compaction scheduler.
     * Call this before closing the KVStore if autoCompact is enabled.
     */
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }
    }

    /**
     * Estimates the fragmentation percentage
     * In append-only storage, fragmentation comes from:
     * - Tombstones (deleted entries)
     * - Obsolete node versions
     */
    private int estimateFragmentation() {
        long totalSize = store.size();
        if (totalSize == 0) return 0;
        
        // Count tombstones by scanning and comparing with file size
        long fileSize = store.getFileSize();
        long expectedSize = totalSize * 64;  // Estimate ~64 bytes per entry
        
        if (expectedSize == 0) return 0;
        
        // Fragmentation = (actual size - expected size) / actual size
        long excessSize = fileSize - expectedSize;
        if (excessSize <= 0) return 0;
        
        return (int) Math.min(100, (excessSize * 100) / fileSize);
    }
}
