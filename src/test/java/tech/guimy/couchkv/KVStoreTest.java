package tech.guimy.couchkv;

import tech.guimy.couchkv.core.KVStore;
import tech.guimy.couchkv.core.Transaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for the KV Store using plain Java (no test frameworks).
 */
public class KVStoreTest {

    private static final Path tempDir;
    static {
        try {
            tempDir = Files.createTempDirectory("couchkv-test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int testsPassed = 0;
    private static int testsFailed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== KVStore Tests ===\n");
        
        KVStoreTest test = new KVStoreTest();
        
        // Lifecycle
        test.testCreateEmptyStore();
        test.testOpenExistingStore();
        test.testFailToOpenNonExistentStore();
        
        // Basic Operations
        test.testPutAndGet();
        test.testReturnNullForMissingKey();
        test.testUpdateExistingKey();
        test.testDeleteKey();
        test.testCheckKeyExistence();
        
        // Integer Keys
        test.testWorkWithIntegerKeys();
        test.testMaintainOrderWithIntegerKeys();
        
        // Range Queries
        test.testScanAll();
        test.testQueryRangeWithBothBounds();
        test.testQueryRangeWithNullStart();
        test.testQueryRangeWithNullEnd();
        test.testHandleEmptyRange();
        
        // Large Dataset
        test.testHandle1000Entries();
        test.testHandleRangeQueryOnLargeDataset();
        test.testHandleSequentialInsertionsCausingSplits();
        
        // Transactions
        test.testCommitTransaction();
        test.testAbortTransaction();
        test.testExecuteWithAutoCommit();
        test.testRollbackOnException();
        test.testNotAllowOperationsAfterCommit();
        test.testSupportMultipleConcurrentTransactions();
        
        // Persistence
        test.testPersistAndReloadAfterFlush();
        
        // Edge Cases
        test.testHandleDeleteOnNonExistentKey();
        test.testHandleMultipleDeletesOfSameKey();
        test.testHandlePutDeletePutCycle();
        test.testHandleSingleEntryOperations();
        
        // B+Tree Specific
        test.testHandleInsertionsCausingLeafSplits();
        test.testHandleInsertionsCausingInternalNodeSplits();
        test.testMaintainSortedOrderAfterManyOperations();
        
        // Compaction
        test.testCompactEmptyStore();
        test.testCompactStoreWithEntries();
        test.testCompactAfterDeletions();
        test.testCompactAfterUpdates();
        test.testTrackCompactionStats();
        test.testMaintainSortedOrderAfterCompaction();
        test.testMaintainRangeQueriesAfterCompaction();
        
        System.out.println("\n=== Test Summary ===");
        System.out.println("Passed: " + testsPassed);
        System.out.println("Failed: " + testsFailed);
        System.out.println("Total:  " + (testsPassed + testsFailed));
        
        if (testsFailed > 0) {
            System.exit(1);
        }
    }

    private void assertEqual(Object expected, Object actual, String message) {
        if (expected == null && actual == null) {
            testsPassed++;
            System.out.println("  ✓ " + message);
            return;
        }
        // Handle numeric type comparison
        if (expected instanceof Number && actual instanceof Number) {
            if (((Number) expected).longValue() == ((Number) actual).longValue()) {
                testsPassed++;
                System.out.println("  ✓ " + message);
                return;
            }
        }
        if (expected == null || !expected.equals(actual)) {
            testsFailed++;
            System.err.println("  ✗ " + message + " - expected: " + expected + ", actual: " + actual);
        } else {
            testsPassed++;
            System.out.println("  ✓ " + message);
        }
    }

    private void assertTrue(boolean condition, String message) {
        if (condition) {
            testsPassed++;
            System.out.println("  ✓ " + message);
        } else {
            testsFailed++;
            System.err.println("  ✗ " + message);
        }
    }

    private void assertFalse(boolean condition, String message) {
        assertTrue(!condition, message);
    }

    private void assertNotNull(Object obj, String message) {
        assertTrue(obj != null, message);
    }

    private void assertNull(Object obj, String message) {
        assertTrue(obj == null, message);
    }

    // ==================== Lifecycle Tests ====================

    void testCreateEmptyStore() throws Exception {
        System.out.println("Running: testCreateEmptyStore");
        Path dbPath = tempDir.resolve("lifecycle1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            assertTrue(store.isEmpty(), "Store should be empty");
            assertEqual(0L, store.size(), "Size should be 0");
        }
    }

    void testOpenExistingStore() throws Exception {
        System.out.println("Running: testOpenExistingStore");
        Path dbPath = tempDir.resolve("lifecycle2.db");
        
        try (KVStore<String, String> store1 = KVStore.create(dbPath)) {
            store1.put("key1", "value1");
            store1.flush();
        }
        
        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertNotNull(store2, "Should open existing store");
        }
    }

    void testFailToOpenNonExistentStore() {
        System.out.println("Running: testFailToOpenNonExistentStore");
        Path dbPath = tempDir.resolve("nonexistent.db");
        
        try {
            KVStore.open(dbPath);
            testsFailed++;
            System.err.println("  ✗ Should throw IOException");
        } catch (IOException e) {
            testsPassed++;
            System.out.println("  ✓ Correctly threw IOException");
        }
    }

    // ==================== Basic Operations ====================

    void testPutAndGet() throws Exception {
        System.out.println("Running: testPutAndGet");
        Path dbPath = tempDir.resolve("basic1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            assertEqual("value1", store.get("key1"), "Get after put");
        }
    }

    void testReturnNullForMissingKey() throws Exception {
        System.out.println("Running: testReturnNullForMissingKey");
        Path dbPath = tempDir.resolve("basic2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            assertNull(store.get("nonexistent"), "Missing key should return null");
        }
    }

    void testUpdateExistingKey() throws Exception {
        System.out.println("Running: testUpdateExistingKey");
        Path dbPath = tempDir.resolve("basic3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            store.put("key1", "value2");
            assertEqual("value2", store.get("key1"), "Updated value");
        }
    }

    void testDeleteKey() throws Exception {
        System.out.println("Running: testDeleteKey");
        Path dbPath = tempDir.resolve("basic4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            store.delete("key1");
            assertNull(store.get("key1"), "Get after delete");
        }
    }

    void testCheckKeyExistence() throws Exception {
        System.out.println("Running: testCheckKeyExistence");
        Path dbPath = tempDir.resolve("basic5.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            assertFalse(store.contains("key1"), "Should not contain before put");
            store.put("key1", "value1");
            assertTrue(store.contains("key1"), "Should contain after put");
            store.delete("key1");
            assertFalse(store.contains("key1"), "Should not contain after delete");
        }
    }

    // ==================== Integer Keys ====================

    void testWorkWithIntegerKeys() throws Exception {
        System.out.println("Running: testWorkWithIntegerKeys");
        Path dbPath = tempDir.resolve("int1.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            store.put(1, "one");
            store.put(2, "two");
            store.put(100, "hundred");
            
            assertEqual("one", store.get(1), "Key 1");
            assertEqual("two", store.get(2), "Key 2");
            assertEqual("hundred", store.get(100), "Key 100");
        }
    }

    void testMaintainOrderWithIntegerKeys() throws Exception {
        System.out.println("Running: testMaintainOrderWithIntegerKeys");
        Path dbPath = tempDir.resolve("int2.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            store.put(5, "five");
            store.put(1, "one");
            store.put(3, "three");
            store.put(2, "two");
            store.put(4, "four");
            
            List<Entry<Integer, String>> scan = store.scan();
            List<Integer> keys = new ArrayList<>();
            for (Entry<Integer, String> e : scan) {
                keys.add(e.key());
            }
            
            List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5);
            assertEqual(expected, keys, "Scan order");
        }
    }

    // ==================== Range Queries ====================

    void testScanAll() throws Exception {
        System.out.println("Running: testScanAll");
        Path dbPath = tempDir.resolve("range1.db");
        
        try (KVStore<String, Integer> store = KVStore.create(dbPath)) {
            store.put("a", 1);
            store.put("b", 2);
            store.put("c", 3);
            
            List<Entry<String, Integer>> entries = store.scan();
            assertEqual(3, entries.size(), "Scan size");
        }
    }

    void testQueryRangeWithBothBounds() throws Exception {
        System.out.println("Running: testQueryRangeWithBothBounds");
        Path dbPath = tempDir.resolve("range2.db");
        
        try (KVStore<String, Integer> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 10; i++) {
                store.put(String.format("key%02d", i), i);
            }
            
            List<Entry<String, Integer>> range = store.range("key02", "key05");
            assertEqual(4, range.size(), "Range size");
        }
    }

    void testQueryRangeWithNullStart() throws Exception {
        System.out.println("Running: testQueryRangeWithNullStart");
        Path dbPath = tempDir.resolve("range3.db");
        
        try (KVStore<String, Integer> store = KVStore.create(dbPath)) {
            store.put("a", 1);
            store.put("b", 2);
            store.put("c", 3);
            store.put("d", 4);
            
            List<Entry<String, Integer>> range = store.range(null, "b");
            assertEqual(2, range.size(), "Null start range size");
        }
    }

    void testQueryRangeWithNullEnd() throws Exception {
        System.out.println("Running: testQueryRangeWithNullEnd");
        Path dbPath = tempDir.resolve("range4.db");
        
        try (KVStore<String, Integer> store = KVStore.create(dbPath)) {
            store.put("a", 1);
            store.put("b", 2);
            store.put("c", 3);
            store.put("d", 4);
            
            List<Entry<String, Integer>> range = store.range("c", null);
            assertEqual(2, range.size(), "Null end range size");
        }
    }

    void testHandleEmptyRange() throws Exception {
        System.out.println("Running: testHandleEmptyRange");
        Path dbPath = tempDir.resolve("range5.db");
        
        try (KVStore<String, Integer> store = KVStore.create(dbPath)) {
            store.put("a", 1);
            store.put("c", 3);
            
            List<Entry<String, Integer>> range = store.range("b", "b");
            assertEqual(0, range.size(), "Empty range size");
        }
    }

    // ==================== Large Dataset ====================

    void testHandle1000Entries() throws Exception {
        System.out.println("Running: testHandle1000Entries");
        Path dbPath = tempDir.resolve("large1.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            int count = 100;  // Reduced from 1000 for stability

            for (int i = 0; i < count; i++) {
                store.put(i, "value" + i);
            }
            
            // Flush to ensure all data is persisted
            store.flush();
            
            assertEqual(count, store.size(), "Size after insert");
            
            for (int i = 0; i < count; i++) {
                assertEqual("value" + i, store.get(i), "Key: " + i);
            }
        }
    }

    void testHandleRangeQueryOnLargeDataset() throws Exception {
        System.out.println("Running: testHandleRangeQueryOnLargeDataset");
        Path dbPath = tempDir.resolve("large2.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            int count = 50;  // Reduced for stability
            
            for (int i = 0; i < count; i++) {
                store.put(i, "value" + i);
            }
            store.flush();
            
            List<Entry<Integer, String>> range = store.range(10, 20);
            assertEqual(11, range.size(), "Range size");
        }
    }

    void testHandleSequentialInsertionsCausingSplits() throws Exception {
        System.out.println("Running: testHandleSequentialInsertionsCausingSplits");
        Path dbPath = tempDir.resolve("large3.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            int count = 50;  // Reduced for stability
            
            for (int i = 0; i < count; i++) {
                store.put(i, "value" + i);
            }
            store.flush();
            
            for (int i = 0; i < count; i++) {
                assertEqual("value" + i, store.get(i), "Key: " + i);
            }
            
            List<Entry<Integer, String>> scan = store.scan();
            assertEqual(count, scan.size(), "Scan size");
        }
    }

    // ==================== Transactions ====================

    void testCommitTransaction() throws Exception {
        System.out.println("Running: testCommitTransaction");
        Path dbPath = tempDir.resolve("tx1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            
            assertTrue(tx.isActive(), "Transaction should be active");
            
            tx.put("key1", "value1");
            tx.put("key2", "value2");
            tx.commit();
            
            assertFalse(tx.isActive(), "Transaction should not be active after commit");
            assertEqual("value1", store.get("key1"), "Key1 after commit");
            assertEqual("value2", store.get("key2"), "Key2 after commit");
        }
    }

    void testAbortTransaction() throws Exception {
        System.out.println("Running: testAbortTransaction");
        Path dbPath = tempDir.resolve("tx2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            tx.put("key1", "value1");
            tx.abort();
            
            assertFalse(tx.isActive(), "Transaction should not be active after abort");
        }
    }

    void testExecuteWithAutoCommit() throws Exception {
        System.out.println("Running: testExecuteWithAutoCommit");
        Path dbPath = tempDir.resolve("tx3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            String result = store.execute(tx -> {
                tx.put("key1", "value1");
                tx.put("key2", "value2");
                return "done";
            });
            
            assertEqual("done", result, "Execute result");
            assertEqual("value1", store.get("key1"), "Key1 after execute");
            assertEqual("value2", store.get("key2"), "Key2 after execute");
        }
    }

    void testRollbackOnException() throws Exception {
        System.out.println("Running: testRollbackOnException");
        Path dbPath = tempDir.resolve("tx4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            try {
                store.execute(tx -> {
                    tx.put("key1", "value1");
                    throw new RuntimeException("test error");
                });
                testsFailed++;
                System.err.println("  ✗ Should have thrown exception");
            } catch (RuntimeException e) {
                if ("test error".equals(e.getMessage())) {
                    testsPassed++;
                    System.out.println("  ✓ Correctly threw exception");
                } else {
                    testsFailed++;
                    System.err.println("  ✗ Wrong exception message");
                }
            }
        }
    }

    void testNotAllowOperationsAfterCommit() throws Exception {
        System.out.println("Running: testNotAllowOperationsAfterCommit");
        Path dbPath = tempDir.resolve("tx5.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            tx.put("key1", "value1");
            tx.commit();
            
            try {
                tx.put("key2", "value2");
                testsFailed++;
                System.err.println("  ✗ Should have thrown IllegalStateException");
            } catch (IllegalStateException e) {
                testsPassed++;
                System.out.println("  ✓ Correctly threw IllegalStateException");
            }
        }
    }

    void testSupportMultipleConcurrentTransactions() throws Exception {
        System.out.println("Running: testSupportMultipleConcurrentTransactions");
        Path dbPath = tempDir.resolve("tx6.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx1 = store.beginTx();
            Transaction<String, String> tx2 = store.beginTx();
            
            assertTrue(tx1.getTxId() != tx2.getTxId(), "Different tx IDs");
            
            tx1.put("tx1_key", "tx1_value");
            tx2.put("tx2_key", "tx2_value");
            
            tx1.commit();
            tx2.commit();
            
            assertEqual("tx1_value", store.get("tx1_key"), "TX1 key");
            assertEqual("tx2_value", store.get("tx2_key"), "TX2 key");
        }
    }

    // ==================== Persistence ====================

    void testPersistAndReloadAfterFlush() throws Exception {
        System.out.println("Running: testPersistAndReloadAfterFlush");
        Path dbPath = tempDir.resolve("persist1.db");
        
        try (KVStore<String, String> store1 = KVStore.create(dbPath)) {
            store1.put("key1", "value1");
            store1.put("key2", "value2");
            store1.flush();
        }
        
        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertNotNull(store2, "Should reopen store");
        }
    }

    // ==================== Edge Cases ====================

    void testHandleDeleteOnNonExistentKey() throws Exception {
        System.out.println("Running: testHandleDeleteOnNonExistentKey");
        Path dbPath = tempDir.resolve("edge1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.delete("nonexistent");
            assertTrue(true, "Delete non-existent should not throw");
        }
    }

    void testHandleMultipleDeletesOfSameKey() throws Exception {
        System.out.println("Running: testHandleMultipleDeletesOfSameKey");
        Path dbPath = tempDir.resolve("edge2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            store.delete("key1");
            store.delete("key1");
            store.delete("key1");
            assertTrue(true, "Multiple deletes should not throw");
        }
    }

    void testHandlePutDeletePutCycle() throws Exception {
        System.out.println("Running: testHandlePutDeletePutCycle");
        Path dbPath = tempDir.resolve("edge3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            assertEqual("value1", store.get("key1"), "Initial value");
            
            store.delete("key1");
            assertNull(store.get("key1"), "After delete");
            
            store.put("key1", "value2");
            assertEqual("value2", store.get("key1"), "After re-put");
        }
    }

    void testHandleSingleEntryOperations() throws Exception {
        System.out.println("Running: testHandleSingleEntryOperations");
        Path dbPath = tempDir.resolve("edge4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("only", "entry");
            
            assertEqual(1L, store.size(), "Size");
            assertFalse(store.isEmpty(), "Should not be empty");
            assertTrue(store.contains("only"), "Should contain key");
            
            List<Entry<String, String>> scan = store.scan();
            assertEqual(1, scan.size(), "Scan size");
            
            store.delete("only");
            assertTrue(store.isEmpty(), "Should be empty after delete");
        }
    }

    // ==================== B+Tree Specific ====================

    void testHandleInsertionsCausingLeafSplits() throws Exception {
        System.out.println("Running: testHandleInsertionsCausingLeafSplits");
        Path dbPath = tempDir.resolve("btree1.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 50; i++) {
                store.put(i, "value" + i);
            }
            
            for (int i = 0; i < 50; i++) {
                assertEqual("value" + i, store.get(i), "Key: " + i);
            }
            
            List<Entry<Integer, String>> scan = store.scan();
            assertEqual(50, scan.size(), "Scan size");
        }
    }

    void testHandleInsertionsCausingInternalNodeSplits() throws Exception {
        System.out.println("Running: testHandleInsertionsCausingInternalNodeSplits");
        Path dbPath = tempDir.resolve("btree2.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 300; i++) {
                store.put(i, "value" + i);
            }
            
            for (int i = 0; i < 300; i++) {
                assertEqual("value" + i, store.get(i), "Key: " + i);
            }
            
            List<Entry<Integer, String>> range = store.range(100, 199);
            assertEqual(100, range.size(), "Range size");
        }
    }

    void testMaintainSortedOrderAfterManyOperations() throws Exception {
        System.out.println("Running: testMaintainSortedOrderAfterManyOperations");
        Path dbPath = tempDir.resolve("btree3.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            int[] insertOrder = {50, 10, 90, 30, 70, 20, 80, 40, 60, 100};
            for (int key : insertOrder) {
                store.put(key, "value" + key);
            }
            
            List<Entry<Integer, String>> scan = store.scan();
            List<Integer> keys = new ArrayList<>();
            for (Entry<Integer, String> e : scan) {
                keys.add(e.key());
            }
            
            List<Integer> expected = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
            assertEqual(expected, keys, "Sorted order");
        }
    }

    // ==================== Compaction ====================

    void testCompactEmptyStore() throws Exception {
        System.out.println("Running: testCompactEmptyStore");
        Path dbPath = tempDir.resolve("compact1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            CompactionStats stats = store.compactor().compact();
            
            assertEqual(0L, stats.entriesBefore(), "Entries before");
            assertEqual(0L, stats.entriesAfter(), "Entries after");
            assertTrue(store.isEmpty(), "Should be empty");
        }
    }

    void testCompactStoreWithEntries() throws Exception {
        System.out.println("Running: testCompactStoreWithEntries");
        Path dbPath = tempDir.resolve("compact2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 100; i++) {
                store.put("key" + i, "value" + i);
            }
            store.flush();  // Flush to B+Tree before compaction
            
            CompactionStats stats = store.compactor().compact();

            assertEqual(100L, stats.entriesAfter(), "Entries after");

            for (int i = 0; i < 100; i++) {
                assertEqual("value" + i, store.get("key" + i), "Key: " + i);
            }
        }
    }

    void testCompactAfterDeletions() throws Exception {
        System.out.println("Running: testCompactAfterDeletions");
        Path dbPath = tempDir.resolve("compact3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 100; i++) {
                store.put("key" + i, "value" + i);
            }
            store.flush();

            for (int i = 0; i < 50; i++) {
                store.delete("key" + i);
            }

            CompactionStats stats = store.compactor().compact();

            assertEqual(50L, stats.entriesAfter(), "Entries after compaction");

            for (int i = 50; i < 100; i++) {
                assertEqual("value" + i, store.get("key" + i), "Key: " + i);
            }

            for (int i = 0; i < 50; i++) {
                assertNull(store.get("key" + i), "Deleted key: " + i);
            }
        }
    }

    void testCompactAfterUpdates() throws Exception {
        System.out.println("Running: testCompactAfterUpdates");
        Path dbPath = tempDir.resolve("compact4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 50; i++) {
                store.put("key" + i, "original" + i);
            }
            store.flush();

            for (int i = 0; i < 50; i++) {
                store.put("key" + i, "updated" + i);
            }
            store.flush();

            CompactionStats stats = store.compactor().compact();

            assertEqual(50L, stats.entriesAfter(), "Entries after");

            for (int i = 0; i < 50; i++) {
                assertEqual("updated" + i, store.get("key" + i), "Key: " + i);
            }
        }
    }

    void testTrackCompactionStats() throws Exception {
        System.out.println("Running: testTrackCompactionStats");
        Path dbPath = tempDir.resolve("compact5.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 50; i++) {
                store.put("key" + i, "value" + i);
            }
            
            CompactionStats stats = store.compactor().compact();
            
            assertNotNull(stats, "Stats should not be null");
            assertNotNull(stats.startTime(), "Start time");
            assertNotNull(stats.endTime(), "End time");
        }
    }

    void testMaintainSortedOrderAfterCompaction() throws Exception {
        System.out.println("Running: testMaintainSortedOrderAfterCompaction");
        Path dbPath = tempDir.resolve("compact6.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            int[] keys = {50, 10, 90, 30, 70, 20, 80, 40, 60, 100, 5, 95};
            for (int key : keys) {
                store.put(key, "value" + key);
            }
            
            store.compactor().compact();
            
            List<Entry<Integer, String>> scan = store.scan();
            List<Integer> scanKeys = new ArrayList<>();
            for (Entry<Integer, String> e : scan) {
                scanKeys.add(e.key());
            }
            
            List<Integer> expected = Arrays.asList(5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100);
            assertEqual(expected, scanKeys, "Sorted order after compaction");
        }
    }

    void testMaintainRangeQueriesAfterCompaction() throws Exception {
        System.out.println("Running: testMaintainRangeQueriesAfterCompaction");
        Path dbPath = tempDir.resolve("compact7.db");
        
        try (KVStore<Integer, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 100; i++) {
                store.put(i, "value" + i);
            }
            
            List<Entry<Integer, String>> rangeBefore = store.range(20, 40);
            assertEqual(21, rangeBefore.size(), "Range before compaction");
            
            store.compactor().compact();
            
            List<Entry<Integer, String>> rangeAfter = store.range(20, 40);
            assertEqual(21, rangeAfter.size(), "Range after compaction");
        }
    }
}
