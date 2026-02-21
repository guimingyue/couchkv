# CouchKV - Append-Only B+Tree KV Store with Transactions

[![Java](https://img.shields.io/badge/Java-24-blue.svg)](https://openjdk.org/projects/jdk/24/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://www.apache.org/licenses/LICENSE-2.0)

A simple, lightweight key-value store implemented in Java with an **append-only B+Tree** and ACID transaction support, inspired by Apache CouchDB's storage architecture.

## Features

- **Append-Only B+Tree**: All writes are appended to the file (no in-place updates)
- **ACID Transactions**: Full transaction support with commit/abort and WAL
- **Write-Ahead Logging (WAL)**: Crash recovery with durable logging
- **MVCC**: Multi-version concurrency control for concurrent reads
- **Range Queries**: Efficient range scans via linked leaf nodes
- **CRC32 Checksums**: Data integrity verification for every block
- **Compaction**: Reclaim space from tombstones and obsolete pages (like CouchDB)
- **MemTable**: In-memory write buffer with automatic flush

## Architecture

```
┌─────────────────────────────────────┐
│           KVStore (API)             │
│  - beginTx() / commit() / abort()   │
│  - put() / get() / delete()         │
│  - range() / scan() / compactor()   │
├─────────────────────────────────────┤
│         Transaction Manager         │
│  - Write set buffering              │
│  - MVCC snapshot isolation          │
├─────────────────────────────────────┤
│        Write-Ahead Log (WAL)        │
│  - Append-only log records          │
│  - CRC32 checksums                  │
│  - Crash recovery                   │
├─────────────────────────────────────┤
│       Append-Only B+Tree            │
│  - Copy-on-write node updates       │
│  - O(log n) operations              │
│  - Leaf node linking for ranges     │
├─────────────────────────────────────┤
│          MemTable                   │
│  - In-memory write buffer           │
│  - Flush to B+Tree on threshold     │
├─────────────────────────────────────┤
│        Compaction Manager           │
│  - Fragmentation detection          │
│  - Space reclamation                │
│  - Tombstone cleanup                │
├─────────────────────────────────────┤
│         Page Manager                │
│  - 4KB header page                  │
│  - Variable-size data blocks        │
│  - CRC32 verification               │
└─────────────────────────────────────┘
```

### File Format

```
+------------------+
| Header (4KB)     |
| - Magic (8)      |  "COUCHKV1"
| - Version (4)    |
| - Root offset (8)|
| - File size (8)  |
| - Seq num (8)    |
| - Timestamp (8)  |
| - CRC32 (4)      |
+------------------+
| Block 1          |
| - Length (4)     |
| - Type (1)       |  LEAF=1, INTERNAL=2, DATA=4
| - SeqNum (8)     |
| - Data (n)       |
| - CRC32 (4)      |
+------------------+
| Block 2          |
+------------------+
| ...              |
+------------------+
| WAL file (.wal)  |
| - Log records    |
+------------------+
```

## Requirements

- **JDK 24** or later
- **Maven 3.6+** for building

## Quick Start

### Basic Usage

```java
import tech.guimy.couchkv.KVStore;
import java.nio.file.Path;

public class Example {
    public static void main(String[] args) throws Exception {
        // Open or create a KV store
        try (KVStore<String, String> store = KVStore.create(Path.of("mydb.kv"))) {

            // Put a value
            store.put("name", "Alice");
            store.put("age", "30");

            // Get a value
            String name = store.get("name");
            System.out.println("Name: " + name);  // Alice

            // Check existence
            if (store.contains("age")) {
                System.out.println("Age exists");
            }

            // Delete a key (null values are treated as deletions)
            store.delete("age");

            // Range query
            store.put("a", "1");
            store.put("b", "2");
            store.put("c", "3");
            store.put("d", "4");

            var range = store.range("b", "c");
            for (var entry : range) {
                System.out.println(entry.key() + " = " + entry.value());
            }

            // Scan all entries
            for (var entry : store.scan()) {
                System.out.println(entry);
            }
        }
    }
}
```

### Transactions

```java
import tech.guimy.couchkv.KVStore;
import tech.guimy.couchkv.Transaction;

try (KVStore<String, String> store = KVStore.create(Path.of("mydb.kv"))) {

    // Begin a transaction
    Transaction<String, String> tx = store.beginTx();

    try {
        tx.put("key1", "value1");
        tx.put("key2", "value2");

        // Commit to persist
        tx.commit();
    } catch (Exception e) {
        // Abort on error
        tx.abort();
        throw e;
    }

    // Or use execute() for auto-commit/rollback
    String result = store.execute(tx -> {
        tx.put("key3", "value3");
        return "done";
    });
}
```

### Compaction

In the append-only B+Tree:
- **Deletes** create tombstones (null values) instead of removing entries
- **Updates** create new versions of nodes (copy-on-write)
- Over time, the file accumulates obsolete data

Compaction reclaims space by:
1. Scanning all active (non-tombstone) entries
2. Building a new compacted B+Tree
3. Replacing the old file with the compacted version

```java
try (KVStore<String, String> store = KVStore.create(Path.of("mydb.kv"))) {

    // Add and delete entries (creates tombstones)
    for (int i = 0; i < 100; i++) {
        store.put("key" + i, "value" + i);
    }
    for (int i = 0; i < 50; i++) {
        store.delete("key" + i);  // Creates tombstones
    }
    store.flush();

    // Check if compaction is needed (30% fragmentation threshold)
    Compactor<String, String> compactor = store.compactor();
    if (compactor.needsCompaction(0.3)) {
        System.out.println("Compaction recommended");
    }

    // Run compaction to reclaim space
    CompactionStats stats = compactor.compact();

    System.out.println("Entries before: " + stats.entriesBefore());
    System.out.println("Entries after: " + stats.entriesAfter());
    System.out.println("Size before: " + stats.sizeBefore() + " bytes");
    System.out.println("Size after: " + stats.sizeAfter() + " bytes");
    System.out.println("Space saved: " + stats.spaceSavedPercent() + "%");
    System.out.println("Duration: " + stats.duration());
}
```

### Integer Keys Example

```java
import tech.guimy.couchkv.KVStore;
import java.nio.file.Path;

try (KVStore<Integer, String> store = KVStore.create(Path.of("mydb.kv"))) {

    // Insert sequential keys
    for (int i = 0; i < 100; i++) {
        store.put(i, "value" + i);
    }
    store.flush();

    // Range query
    var range = store.range(10, 20);
    System.out.println("Found " + range.size() + " entries");

    // Get specific key
    String value = store.get(42);
    System.out.println("Key 42 = " + value);
}
```

## API Reference

### KVStore

| Method | Description |
|--------|-------------|
| `put(K key, V value)` | Insert or update a key-value pair (null value = delete) |
| `get(K key)` | Get value by key (returns null if not found) |
| `delete(K key)` | Delete a key |
| `contains(K key)` | Check if key exists |
| `range(K start, K end)` | Get entries in range [start, end] (null = unbounded) |
| `scan()` | Get all entries |
| `beginTx()` | Begin a new transaction |
| `execute(Function<Transaction, T>)` | Execute operation in transaction (auto-commit) |
| `read(Function<KVStore, T>)` | Execute read-only operation |
| `compactor()` | Get the compactor for this store |
| `flush()` | Flush all data to disk |
| `size()` | Get approximate entry count |
| `isEmpty()` | Check if store is empty |

### Transaction

| Method | Description |
|--------|-------------|
| `get(K key)` | Get value within transaction |
| `put(K key, V value)` | Put value within transaction |
| `delete(K key)` | Delete key within transaction |
| `commit()` | Commit the transaction |
| `abort()` | Abort the transaction |
| `getStatus()` | Get transaction status (ACTIVE/COMMITTED/ABORTED) |
| `isActive()` | Check if transaction is active |
| `getTxId()` | Get unique transaction ID |

### Compactor

| Method | Description |
|--------|-------------|
| `compact()` | Run compaction and return statistics |
| `isCompacting()` | Check if compaction is in progress |
| `getLastStats()` | Get statistics from last compaction |
| `getLastCompaction()` | Get timestamp of last compaction |
| `needsCompaction(double threshold)` | Check if compaction is recommended |
| `needsCompaction()` | Check with default 30% threshold |
| `shutdown()` | Shutdown compaction scheduler |

### CompactionStats

| Method | Description |
|--------|-------------|
| `entriesBefore()` | Number of entries before compaction |
| `entriesAfter()` | Number of entries after compaction |
| `entriesRemoved()` | Number of entries removed |
| `sizeBefore()` | Estimated size before compaction (bytes) |
| `sizeAfter()` | Estimated size after compaction (bytes) |
| `spaceSavedPercent()` | Percentage of space saved |
| `duration()` | Duration of compaction |
| `startTime()` | When compaction started |
| `endTime()` | When compaction completed |
| `fragmentationBefore()` | Fragmentation % before compaction |
| `fragmentationAfter()` | Fragmentation % after compaction |

### TxStatus

| Status | Description |
|--------|-------------|
| `ACTIVE` | Transaction is in progress |
| `COMMITTED` | Transaction has been committed |
| `ABORTED` | Transaction has been aborted |

## Data Format

Keys and values must be `Serializable`. Supported types:

- **String**: `"key"`, `"value"` (unlimited size)
- **Integer**: `1`, `2`, `3`
- **Long**: `1000000L`
- **Double**: `3.14`
- **Custom**: Any class implementing `Serializable`

```java
// Custom serializable value
record User(String name, int age) implements Serializable {}

try (KVStore<String, User> store = KVStore.create(Path.of("users.kv"))) {
    store.put("user1", new User("Alice", 30));
    User user = store.get("user1");
}
```

## Building

```bash
# Build with Maven
mvn clean install

# Run tests
mvn test

# Run example
mvn exec:java -Dexec.mainClass="tech.guimy.couchkv.Example"
```

## Testing

The project includes comprehensive test suites:

| Test Class | Tests | Description |
|------------|-------|-------------|
| `KVStoreTest` | 812 | Basic CRUD, transactions, B+Tree operations |
| `WALTest` | 124 | Write-ahead log and recovery tests |
| `CouchKVComprehensiveTest` | 1,488 | Concurrency, compaction, edge cases, stress tests |

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=KVStoreTest
mvn test -Dtest=WALTest
mvn test -Dtest=CouchKVComprehensiveTest
```

## Comparison

| Feature | CouchKV | Redis | LevelDB |
|---------|---------|-------|---------|
| B+Tree | ✅ | ❌ (hash/skip) | ✅ (LSM) |
| Transactions | ✅ | ❌ | ❌ |
| Range Queries | ✅ | ✅ | ✅ |
| Persistence | ✅ | Optional | ✅ |
| WAL | ✅ | ❌ (AOF) | ✅ |
| Compaction | ✅ | ❌ | ✅ |
| In-Memory | Partial | ✅ | ❌ |
| Language | Java | C | C++ |

## Package Structure

```
tech.guimy.couchkv/
├── KVStore.java           # Main API with B+Tree and WAL
├── Transaction.java       # ACID transaction support
├── TxStatus.java          # Transaction status enum
├── Compactor.java         # Compaction manager
├── CompactionStats.java   # Compaction statistics
├── Entry.java             # Key-value entry record
├── BTreeNode.java         # B+Tree base class
├── BTreeLeafNode.java     # Leaf nodes with linked list
├── BTreeInternalNode.java # Internal index nodes
└── Example.java           # Usage example
```

## Implementation Notes

### Append-Only Design
- All data blocks are appended to the file (no in-place updates)
- Old versions remain until compaction
- Copy-on-write semantics for B+Tree nodes

### WAL Recovery
- WAL records are replayed on startup
- Corrupted WAL records are skipped gracefully
- Committed transactions are recovered
- Uncommitted transactions are discarded

### Null Value Handling
- `put(key, null)` is treated as a deletion
- `ConcurrentHashMap` doesn't allow null values

### CRC32 Checksums
- Every block includes CRC32 checksum
- Checksums verified on read
- Corrupted blocks are detected and skipped

## License

Apache License 2.0
