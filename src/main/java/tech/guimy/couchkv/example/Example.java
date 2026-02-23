package tech.guimy.couchkv.example;

import tech.guimy.couchkv.KVStore;

import java.nio.file.Path;

/**
 * Simple example demonstrating KV Store usage.
 */
public class Example {

    public static void main(String[] args) throws Exception {
        System.out.println("=== CouchKV Example ===\n");

        Path dbPath = Path.of("example.kv");

        try (KVStore<String, String> store = KVStore.create(dbPath)) {

            // Basic operations
            System.out.println("1. Basic Operations");
            store.put("name", "Alice");
            store.put("email", "alice@example.com");
            store.put("age", "30");

            System.out.println("   name = " + store.get("name"));
            System.out.println("   email = " + store.get("email"));
            System.out.println("   contains 'age': " + store.contains("age"));

            // Update
            System.out.println("\n2. Update");
            store.put("age", "31");
            System.out.println("   age = " + store.get("age"));

            // Range query
            System.out.println("\n3. Range Query");
            store.put("a", "apple");
            store.put("b", "banana");
            store.put("c", "cherry");
            store.put("d", "date");

            var range = store.range("b", "c");
            System.out.println("   Range [b, c]:");
            for (var entry : range) {
                System.out.println("     " + entry.key() + " = " + entry.value());
            }

            // Transaction
            System.out.println("\n4. Transaction");
            var tx = store.beginTx();
            System.out.println("   Transaction status: " + tx.getStatus());

            tx.put("tx_key1", "tx_value1");
            tx.put("tx_key2", "tx_value2");
            tx.commit();

            System.out.println("   After commit: " + tx.getStatus());
            System.out.println("   tx_key1 = " + store.get("tx_key1"));

            // Execute with auto-commit
            System.out.println("\n5. Execute with Auto-commit");
            String result = store.execute(tx2 -> {
                tx2.put("auto_key", "auto_value");
                return "completed";
            });
            System.out.println("   Result: " + result);
            System.out.println("   auto_key = " + store.get("auto_key"));

            // Scan all
            System.out.println("\n6. Scan All Entries");
            for (var entry : store.scan()) {
                System.out.println("   " + entry);
            }

            // Delete
            System.out.println("\n7. Delete");
            store.delete("a");
            System.out.println("   Deleted 'a', contains: " + store.contains("a"));

            // Stats
            System.out.println("\n8. Stats");
            System.out.println("   Size: " + store.size());
            System.out.println("   Empty: " + store.isEmpty());

            // Flush
            store.flush();
            System.out.println("   Data flushed to disk");

            System.out.println("\n=== Example Complete ===");
        }

        // Cleanup
        System.out.println("\nCleaning up example database...");
        java.io.File dbFile = dbPath.toFile();
        if (dbFile.exists()) {
            dbFile.delete();
        }
        java.io.File walFile = Path.of("example.kv.wal").toFile();
        if (walFile.exists()) {
            walFile.delete();
        }
    }
}
