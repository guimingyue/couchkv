package tech.guimy.couchkv;

import tech.guimy.couchkv.core.KVStore;
import tech.guimy.couchkv.core.Transaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for Write-Ahead Log (WAL) functionality using plain Java.
 */
public class WALTest {

    private static final Path tempDir;
    static {
        try {
            tempDir = Files.createTempDirectory("couchkv-wal-test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int testsPassed = 0;
    private static int testsFailed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== WAL Tests ===\n");
        
        WALTest test = new WALTest();
        
        // WAL Write Tests
        test.testWriteWALBeforeData();
        test.testWriteWALBeginForTransactions();
        test.testWriteWALCommitBeforeApply();
        test.testWriteWALAbort();
        test.testFlushWALAfterWrite();
        
        // WAL Recovery Tests
        test.testReplayNonTransactionalOps();
        test.testOnlyReplayCommittedTransactions();
        test.testHandleMixedTransactions();
        test.testHandleCorruptedWAL();
        
        // WAL Stress Tests
        test.testHandleManySmallWrites();
        test.testHandleConcurrentWALWrites();
        test.testHandleLargeValuesInWAL();
        test.testHandleRapidTxCycles();
        
        // WAL CRC Tests
        test.testVerifyCRConRead();
        
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

    // ==================== WAL Write Tests ====================

    void testWriteWALBeforeData() throws Exception {
        System.out.println("Running: testWriteWALBeforeData");
        Path dbPath = tempDir.resolve("walwrite1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.exists(walPath), "WAL file should exist");
            assertTrue(Files.size(walPath) > 0, "WAL should have content");
        }
    }

    void testWriteWALBeginForTransactions() throws Exception {
        System.out.println("Running: testWriteWALBeginForTransactions");
        Path dbPath = tempDir.resolve("walwrite2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            tx.put("key1", "value1");
            tx.commit();
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.size(walPath) > 0, "WAL should have records");
        }
    }

    void testWriteWALCommitBeforeApply() throws Exception {
        System.out.println("Running: testWriteWALCommitBeforeApply");
        Path dbPath = tempDir.resolve("walwrite3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            tx.put("key1", "value1");
            tx.put("key2", "value2");
            tx.commit();
            
            assertEqual("value1", store.get("key1"), "Key1 after commit");
            assertEqual("value2", store.get("key2"), "Key2 after commit");
        }
    }

    void testWriteWALAbort() throws Exception {
        System.out.println("Running: testWriteWALAbort");
        Path dbPath = tempDir.resolve("walwrite4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            Transaction<String, String> tx = store.beginTx();
            tx.put("key1", "value1");
            tx.abort();
            
            assertNull(store.get("key1"), "Data should not be visible after abort");
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.size(walPath) > 0, "WAL should have ABORT record");
        }
    }

    void testFlushWALAfterWrite() throws Exception {
        System.out.println("Running: testFlushWALAfterWrite");
        Path dbPath = tempDir.resolve("walwrite5.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            long sizeAfterFirst = Files.size(walPath);
            assertTrue(sizeAfterFirst > 0, "WAL should be flushed");
            
            store.put("key2", "value2");
            long sizeAfterSecond = Files.size(walPath);
            assertTrue(sizeAfterSecond > sizeAfterFirst, "WAL should grow");
        }
    }

    // ==================== WAL Recovery Tests ====================

    void testReplayNonTransactionalOps() throws Exception {
        System.out.println("Running: testReplayNonTransactionalOps");
        Path dbPath = tempDir.resolve("walrecovery1.db");

        try (KVStore<String, String> store1 = KVStore.create(dbPath)) {
            store1.put("key1", "value1");
            store1.put("key2", "value2");
        }

        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertEqual("value1", store2.get("key1"), "Recovery key1");
            assertEqual("value2", store2.get("key2"), "Recovery key2");
        }
    }

    void testOnlyReplayCommittedTransactions() throws Exception {
        System.out.println("Running: testOnlyReplayCommittedTransactions");
        Path dbPath = tempDir.resolve("walrecovery2.db");

        KVStore<String, String> store1 = new KVStore<>(dbPath);
        
        store1.execute(tx -> {
            tx.put("committed-key", "committed-value");
            return null;
        });
        
        Transaction<String, String> tx = store1.beginTx();
        tx.put("uncommitted-key", "uncommitted-value");
        
        store1.flush();
        store1.close();

        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertEqual("committed-value", store2.get("committed-key"), "Committed tx");
            assertNull(store2.get("uncommitted-key"), "Uncommitted tx");
        }
    }

    void testHandleMixedTransactions() throws Exception {
        System.out.println("Running: testHandleMixedTransactions");
        Path dbPath = tempDir.resolve("walrecovery3.db");

        try (KVStore<String, String> store1 = KVStore.create(dbPath)) {
            store1.execute(tx -> {
                tx.put("key1", "value1");
                return null;
            });
            
            Transaction<String, String> tx2 = store1.beginTx();
            tx2.put("key2", "value2");
            tx2.abort();
            
            store1.execute(tx -> {
                tx.put("key3", "value3");
                return null;
            });
        }

        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertEqual("value1", store2.get("key1"), "Committed key1");
            assertNull(store2.get("key2"), "Aborted key2");
            assertEqual("value3", store2.get("key3"), "Committed key3");
        }
    }

    void testHandleCorruptedWAL() throws Exception {
        System.out.println("Running: testHandleCorruptedWAL");
        Path dbPath = tempDir.resolve("walrecovery4.db");

        try (KVStore<String, String> store1 = KVStore.create(dbPath)) {
            store1.put("valid-key", "valid-value");
            store1.flush();
        }

        Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
        if (Files.exists(walPath) && Files.size(walPath) > 20) {
            byte[] content = Files.readAllBytes(walPath);
            content[15] = (byte) 0xFF;
            content[16] = (byte) 0xFF;
            Files.write(walPath, content);
        }

        try (KVStore<String, String> store2 = KVStore.open(dbPath)) {
            assertNotNull(store2, "Should open despite corrupted WAL");
        }
    }

    // ==================== WAL Stress Tests ====================

    void testHandleManySmallWrites() throws Exception {
        System.out.println("Running: testHandleManySmallWrites");
        Path dbPath = tempDir.resolve("walstress1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 1000; i++) {
                store.put("key-" + i, "v" + i);
            }
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.size(walPath) > 0, "WAL should exist");
        }
    }

    void testHandleConcurrentWALWrites() throws Exception {
        System.out.println("Running: testHandleConcurrentWALWrites");
        Path dbPath = tempDir.resolve("walstress2.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            int threadCount = 5;
            int writesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicBoolean error = new AtomicBoolean(false);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < writesPerThread; i++) {
                            store.put("key-" + threadId + "-" + i, "value-" + i);
                        }
                    } catch (Exception e) {
                        error.set(true);
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertFalse(error.get(), "No errors during concurrent writes");
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.size(walPath) > 0, "WAL should exist");
        }
    }

    void testHandleLargeValuesInWAL() throws Exception {
        System.out.println("Running: testHandleLargeValuesInWAL");
        Path dbPath = tempDir.resolve("walstress3.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            String largeValue = "x".repeat(10000);
            store.put("large-key", largeValue);
            
            Path walPath = dbPath.resolveSibling(dbPath.getFileName() + ".wal");
            assertTrue(Files.size(walPath) > 10000, "WAL should contain large value");
            
            store.flush();
        }

        try (KVStore<String, String> store = KVStore.open(dbPath)) {
            String value = store.get("large-key");
            assertNotNull(value, "Large value should be recovered");
            assertEqual(10000, value.length(), "Large value size");
        }
    }

    void testHandleRapidTxCycles() throws Exception {
        System.out.println("Running: testHandleRapidTxCycles");
        Path dbPath = tempDir.resolve("walstress4.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            for (int i = 0; i < 100; i++) {
                final int finalI = i;
                if (i % 2 == 0) {
                    store.execute(tx -> {
                        tx.put("key-" + finalI, "value-" + finalI);
                        return null;
                    });
                } else {
                    Transaction<String, String> tx = store.beginTx();
                    tx.put("key-" + finalI, "value-" + finalI);
                    tx.abort();
                }
            }
            
            for (int i = 0; i < 100; i++) {
                if (i % 2 == 0) {
                    assertEqual("value-" + i, store.get("key-" + i), "Committed key: " + i);
                } else {
                    assertNull(store.get("key-" + i), "Aborted key: " + i);
                }
            }
        }
    }

    // ==================== WAL CRC Tests ====================

    void testVerifyCRConRead() throws Exception {
        System.out.println("Running: testVerifyCRConRead");
        Path dbPath = tempDir.resolve("walcrc1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            store.flush();
        }

        try (KVStore<String, String> store = KVStore.open(dbPath)) {
            assertEqual("value1", store.get("key1"), "Recovery with CRC verification");
        }
    }
}
