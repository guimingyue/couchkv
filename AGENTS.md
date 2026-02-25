# AGENTS.md - Coding Agent Guide for CouchKV

## Project Overview

CouchKV is a Java key-value store with B+Tree indexing and ACID transactions. It features an append-only storage architecture with WAL (Write-Ahead Log), MVCC, and compaction support.

## Build/Lint/Test Commands

```bash
# Build project
mvn clean install

# Compile only (no tests)
mvn compile

# Run all tests
mvn test

# Run a specific test class
mvn test -Dtest=KVStoreTest
mvn test -Dtest=WALTest
mvn test -Dtest=CouchKVComprehensiveTest

# Run a single test method
mvn test -Dtest=KVStoreTest#testPutAndGet

# Package without running tests
mvn package -DskipTests

# Run the example
mvn exec:java -Dexec.mainClass="tech.guimy.couchkv.example.Example"
```

## Project Structure

```
src/main/java/tech/guimy/couchkv/
├── CouchKV.java              # Main API (RocksDB-style facade)
├── KVStore.java              # Core KV store (in core/ subpackage)
├── Transaction.java          # ACID transaction support (in core/)
├── Entry.java                # Key-value entry record
├── TxStatus.java             # Transaction status enum
├── CompactionStats.java      # Compaction statistics
├── Options.java              # Database configuration options
├── WriteBatch.java           # Batch write operations
├── KVIterator.java           # Iterator for scanning
└── core/
    ├── BTreeNode.java        # B+Tree base class
    ├── BTreeLeafNode.java    # Leaf nodes
    ├── BTreeInternalNode.java# Internal index nodes
    ├── Compactor.java        # Compaction manager
    └── KVStore.java          # Core implementation

src/test/java/tech/guimy/couchkv/
├── KVStoreTest.java          # Basic CRUD and transaction tests
├── WALTest.java              # Write-ahead log tests
└── CouchKVComprehensiveTest.java # Comprehensive test suite
```

## Code Style Guidelines

### Imports

Order imports as follows, separated by blank lines:
1. `java.*` and `javax.*` imports (alphabetically)
2. Third-party imports
3. `tech.guimy.couchkv.*` imports

```java
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import tech.guimy.couchkv.Entry;
import tech.guimy.couchkv.core.KVStore;
```

Avoid wildcard imports in production code; use explicit imports.

### Formatting

- **Indentation**: 4 spaces (no tabs)
- **Line length**: 120 characters max
- **Braces**: K&R style (opening brace on same line, except for class/method declarations)
- **Blank lines**: 
  - One blank line between methods
  - Use section comments for logical groupings: `// ==================== Section Name ====================`
- **Javadoc**: Required for all public classes and public methods

### Types and Generics

Key types use bounded generics:
```java
public class KVStore<K extends Serializable & Comparable<K>, V extends Serializable>
```

Use records for immutable data classes:
```java
public record Entry<K extends Serializable & Comparable<K>, V extends Serializable>(
    K key, 
    V value
) implements Serializable { }
```

Use enums for fixed sets of values:
```java
public enum TxStatus {
    ACTIVE,
    COMMITTED,
    ABORTED
}
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `KVStore`, `BTreeNode`, `Transaction` |
| Interfaces | PascalCase | `Serializable` |
| Methods | camelCase | `beginTx()`, `flushMemtable()`, `getFileSize()` |
| Constants | UPPER_SNAKE_CASE | `HEADER_SIZE`, `BLOCK_TYPE_LEAF`, `MAGIC` |
| Private fields | camelCase | `memTable`, `walChannel`, `txIdGenerator` |
| Parameters | camelCase | `fragmentationThreshold`, `isolationLevel` |
| Type parameters | Single letter or short name | `K`, `V`, `T` |

### Error Handling

- Declare checked exceptions: `throws IOException`
- Throw `IllegalStateException` for invalid states (e.g., using a closed transaction)
- Throw `FileNotFoundException` for missing database files
- Use descriptive error messages

```java
private void checkActive() {
    if (status != TxStatus.ACTIVE) {
        throw new IllegalStateException("Transaction is not active: " + status);
    }
}
```

### Comments

- **No inline comments** unless absolutely necessary for clarification
- Use Javadoc for public APIs:
```java
/**
 * Begins a new transaction with the specified isolation level.
 * 
 * @param isolationLevel the transaction isolation level
 * @return a new transaction
 */
public Transaction<K, V> beginTx(TransactionIsolationLevel isolationLevel) {
```

- Use section headers for organizing code:
```java
// ==================== Transaction API ====================
// ==================== B+Tree Operations ====================
// ==================== WAL Implementation ====================
```

### Testing

Tests use plain Java with custom assertion methods (no JUnit annotations in existing tests):

```java
public class KVStoreTest {
    private static int testsPassed = 0;
    private static int testsFailed = 0;

    public static void main(String[] args) throws Exception {
        KVStoreTest test = new KVStoreTest();
        test.testPutAndGet();
        // ...
    }

    void testPutAndGet() throws Exception {
        System.out.println("Running: testPutAndGet");
        Path dbPath = tempDir.resolve("basic1.db");
        
        try (KVStore<String, String> store = KVStore.create(dbPath)) {
            store.put("key1", "value1");
            assertEqual("value1", store.get("key1"), "Get after put");
        }
    }

    private void assertEqual(Object expected, Object actual, String message) {
        // ...
    }
}
```

Test naming convention: `test` + `DescriptionInCamelCase`
- `testPutAndGet`
- `testCommitTransaction`
- `testCompactEmptyStore`

### Resource Management

Always use try-with-resources for `AutoCloseable` resources:

```java
try (KVStore<String, String> store = KVStore.create(dbPath)) {
    store.put("key", "value");
}
```

### Locking Pattern

Use `ReentrantReadWriteLock` for thread-safe operations:

```java
public V get(K key) {
    lock.readLock().lock();
    try {
        // read operation
        return result;
    } finally {
        lock.readLock().unlock();
    }
}

public void put(K key, V value) throws IOException {
    lock.writeLock().lock();
    try {
        // write operation
    } finally {
        lock.writeLock().unlock();
    }
}
```

### Key Implementation Notes

1. **Append-Only Storage**: All writes append to the file; no in-place updates
2. **WAL**: Write-ahead log records must be written and flushed BEFORE data modification
3. **Null Values**: `put(key, null)` is treated as deletion; ConcurrentHashMap doesn't allow null values
4. **CRC32 Checksums**: Every block includes a CRC32 checksum for integrity
5. **Serialization**: Supports String, Integer, Long, byte[], and custom Serializable objects

## Dependencies

- Java 24 (JDK 24 required)
- Maven 3.6+
- JUnit Jupiter 5.11.4 (test scope)
- AssertJ 3.27.3 (test scope)