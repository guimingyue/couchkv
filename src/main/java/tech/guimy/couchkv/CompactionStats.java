package tech.guimy.couchkv;

import java.time.Instant;

/**
 * Compaction statistics
 */
public record CompactionStats(
    Instant startTime,
    Instant endTime,
    long entriesBefore,
    long entriesAfter,
    long sizeBefore,
    long sizeAfter,
    int fragmentationBefore,
    int fragmentationAfter
) {
    /**
     * @return percentage of space saved
     */
    public double spaceSavedPercent() {
        if (sizeBefore == 0) return 0;
        return (double) (sizeBefore - sizeAfter) / sizeBefore * 100;
    }
    
    /**
     * @return number of entries removed (tombstones cleaned up)
     */
    public long entriesRemoved() {
        return entriesBefore - entriesAfter;
    }
    
    /**
     * @return duration of compaction
     */
    public java.time.Duration duration() {
        return java.time.Duration.between(startTime, endTime);
    }
}
