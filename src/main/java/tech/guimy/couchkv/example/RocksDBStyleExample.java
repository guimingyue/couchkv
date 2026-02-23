package tech.guimy.couchkv.example;

import tech.guimy.couchkv.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating RocksDB-like API usage.
 */
public class RocksDBStyleExample {

    public static void main(String[] args) throws Exception {
        System.out.println("=== CouchKV RocksDB-Style API Example ===\n");

        Path dbPath = java.nio.file.Files.createTempDirectory("couchkv").resolve("example_rocksdb.kv");

        // Open database with options
        Options options = new Options()
            .setCreateIfMissing(true)
            .setAutoCompact(true)
            .setFragmentationThreshold(0.3);

        try (CouchKV db = CouchKV.open(dbPath, options)) {

            // 1. Basic Put/Get
            System.out.println("1. Basic Put/Get");
            db.put("key1".getBytes(), "value1".getBytes());
            db.put("key2".getBytes(), "value2".getBytes());

            byte[] value = db.get("key1".getBytes());
            System.out.println("   get(key1) = " + new String(value));

            // 2. String API
            System.out.println("\n2. String API");
            db.put("name", "Alice");
            String name = db.getString("name");
            System.out.println("   getString(name) = " + name);

            // 3. Multi-Get
            System.out.println("\n3. Multi-Get");
            List<byte[]> keys = Arrays.asList(
                "key1".getBytes(),
                "key2".getBytes(),
                "key3".getBytes()
            );
            Map<byte[], byte[]> results = db.multiGet(keys);
            System.out.println("   multiGet found " + results.size() + " entries");

            // 4. Write Batch (Atomic)
            System.out.println("\n4. Write Batch");
            try (WriteBatch batch = db.createWriteBatch()) {
                batch.put("batch1", "value1");
                batch.put("batch2", "value2");
                batch.delete("key2");
                db.write(batch);
                System.out.println("   Wrote batch with 3 operations");
            }

            // 5. Write with Options
            System.out.println("\n5. Write with Sync");
            WriteOptions writeOptions = new WriteOptions().setSync(true);
            db.put("sync_key".getBytes(), "sync_value".getBytes(), writeOptions);
            System.out.println("   Wrote with sync=true");

            // 6. Iteration
            System.out.println("\n6. Iteration");
            try (KVIterator iter = db.newIterator()) {
                int count = 0;
                while (iter.isValid()) {
                    count++;
                    iter.next();
                }
                System.out.println("   Iterated over " + count + " entries");
            }

            // 7. Range Query
            System.out.println("\n7. Range Query");
            try (KVIterator iter = db.newIterator("a".getBytes(), "z".getBytes())) {
                int count = 0;
                while (iter.isValid()) {
                    count++;
                    iter.next();
                }
                System.out.println("   Range [a,z] contains " + count + " entries");
            }

            // 8. Compaction
            System.out.println("\n8. Compaction");
            db.compactRange();
            CompactionStats stats = db.getCompactionStats();
            if (stats != null) {
                System.out.println("   Compaction saved " + stats.spaceSavedPercent() + "% space");
            }

            // 9. Properties
            System.out.println("\n9. Properties");
            System.out.println("   Num keys: " + db.getProperty(CouchKV.PROPERTY_NUM_KEYS));
            System.out.println("   File size: " + db.getProperty(CouchKV.PROPERTY_FILE_SIZE) + " bytes");
            System.out.println("   Approx num keys: " + db.getApproximateNumKeys());

            // 10. Delete
            System.out.println("\n10. Delete");
            db.delete("key1");
            System.out.println("   Deleted key1");

            // 11. Flush
            System.out.println("\n11. Flush");
            db.flush();
            System.out.println("   Flushed to disk");
        }

        // Re-open and verify
        System.out.println("\n12. Re-open Database");
        try (CouchKV db = CouchKV.open(dbPath)) {
            String name = db.getString("name");
            System.out.println("   Re-opened, name = " + name);
        }

        // Cleanup
        System.out.println("\n=== Cleaning up ===");
        java.nio.file.Files.deleteIfExists(dbPath);
        java.nio.file.Files.deleteIfExists(dbPath.resolveSibling(dbPath.getFileName() + ".wal"));
        System.out.println("Done!");
    }
}
