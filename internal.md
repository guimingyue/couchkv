# CouchKV Internal Implementation Details

This document describes the internal implementation details of CouchKV, a Java-based append-only B+Tree key-value store with ACID transaction support.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Storage Layer](#storage-layer)
3. [B+Tree Implementation](#btree-implementation)
4. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
5. [Transaction Management](#transaction-management)
6. [MemTable](#memtable)
7. [Compaction](#compaction)
8. [Data Integrity](#data-integrity)
9. [Concurrency Control](#concurrency-control)
10. [File Format](#file-format)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    CouchKV API                          │
│  (RocksDB-style: put, get, delete, iterator, batch)     │
├─────────────────────────────────────────────────────────┤
│                   Transaction Layer                     │
│  (Write sets, MVCC, commit/abort handling)              │
├─────────────────────────────────────────────────────────┤
│                    MemTable                             │
│  (ConcurrentHashMap, write buffer, auto-flush)          │
├─────────────────────────────────────────────────────────┤
│                   B+Tree Index                          │
│  (In-memory, copy-on-write, leaf linking)               │
├─────────────────────────────────────────────────────────┤
│              Write-Ahead Log (WAL)                      │
│  (Append-only, CRC32, crash recovery)                   │
├─────────────────────────────────────────────────────────┤
│               Compaction Manager                        │
│  (Fragmentation detection, space reclamation)           │
├─────────────────────────────────────────────────────────┤
│                  File I/O Layer                         │
│  (Append-only blocks, CRC32, memory-mapped reads)       │
└─────────────────────────────────────────────────────────┘
```

---

## Storage Layer

### Append-Only Design

CouchKV uses an **append-only** storage model inspired by Apache CouchDB:

1. **No in-place updates**: All writes are appended to the end of the file
2. **Copy-on-write**: Modified data creates new blocks, old blocks remain until compaction
3. **Crash safety**: Partial writes don't corrupt existing data

### Block Structure

Each data block has the following format:

```
+------------------+
| Length (4 bytes) |  Total block length
+------------------+
| Type (1 byte)    |  LEAF=1, INTERNAL=2, DATA=4
+------------------+
| SeqNum (8 bytes) |  Sequence number for ordering
+------------------+
| Data (variable)  |  Serialized node or value data
+------------------+
| CRC32 (4 bytes)  |  Checksum of type + seq + data
+------------------+
```

### File I/O Operations

```java
// Append a block to the file
private long appendBlock(byte type, byte[] data) throws IOException {
    int blockLen = 4 + 1 + 8 + data.length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(blockLen);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    
    buffer.putInt(blockLen);      // Length
    buffer.put(type);             // Type
    buffer.putLong(seq);          // Sequence number
    buffer.put(data);             // Data
    
    // Compute and append CRC32
    CRC32 crc32 = new CRC32();
    crc32.update(type);
    crc32.update(seqBytes);
    crc32.update(data);
    buffer.putInt((int) crc32.getValue());
    
    // Append to file
    long offset = fileSize;
    dataChannel.write(buffer, offset);
    fileSize += blockLen;
    
    return offset;
}
```

---

## B+Tree Implementation

### Tree Structure

CouchKV implements an in-memory B+Tree with the following characteristics:

- **Order**: 32 (max 31 keys per node)
- **Leaf nodes**: Store actual key-value pairs, linked for range scans
- **Internal nodes**: Store keys and child pointers only
- **Copy-on-write**: Modifications create new node versions

### Node Classes

```java
// Base node class
abstract class BTreeNode<K, V> {
    protected List<K> keys;
    protected long fileOffset;
    protected long sequenceNumber;
    
    abstract V get(K key);
    abstract SplitResult insert(K key, V value);
    abstract void delete(K key);
    abstract List<Entry<K, V>> range(K start, K end);
}

// Leaf node - stores key-value pairs
class BTreeLeafNode<K, V> extends BTreeNode<K, V> {
    List<V> values;
    BTreeNode<K, V> nextLeaf;  // Linked list pointer
}

// Internal node - stores keys and child pointers
class BTreeInternalNode<K, V> extends BTreeNode<K, V> {
    List<BTreeNode<K, V>> children;
}
```

### Insert Operation

```java
// Insert into leaf node
SplitResult<K, V> insert(K key, V value) {
    int idx = findKeyIndex(key);
    
    if (idx >= 0) {
        // Key exists - update value
        values.set(idx, value);
        return null;
    }
    
    // Insert new key-value pair
    int insertPos = -(idx) - 1;
    keys.add(insertPos, key);
    values.add(insertPos, value);
    
    // Split if full
    if (isFull()) {
        return split();
    }
    return null;
}

// Node split
SplitResult<K, V> split() {
    int mid = keys.size() / 2;
    BTreeLeafNode<K, V> right = new BTreeLeafNode<>();
    
    // Move upper half to right node
    for (int i = keys.size() - 1; i >= mid; i--) {
        right.keys.add(0, keys.remove(i));
        right.values.add(0, values.remove(i));
    }
    
    // Link leaves
    right.nextLeaf = this.nextLeaf;
    this.nextLeaf = right;
    
    return new SplitResult<>(right.keys.get(0), right);
}
```

### Range Query

Range queries use the leaf node linked list:

```java
List<Entry<K, V>> range(K start, K end) {
    List<Entry<K, V>> results = new ArrayList<>();
    
    // Find start position
    int startPos = findStartIndex(start);
    
    // Iterate through leaf chain
    BTreeLeafNode<K, V> leaf = this;
    while (leaf != null) {
        for (int i = startPos; i < leaf.keys.size(); i++) {
            K key = leaf.keys.get(i);
            
            if (end != null && key.compareTo(end) > 0) {
                return results;  // Past end range
            }
            
            if (leaf.values.get(i) != null) {
                results.add(new Entry<>(key, leaf.values.get(i)));
            }
        }
        
        leaf = (BTreeLeafNode<K, V>) leaf.nextLeaf;
        startPos = 0;
    }
    
    return results;
}
```

---

## Write-Ahead Log (WAL)

### WAL Record Format

```
+------------------+
| Length (4 bytes) |
+------------------+
| txId (8 bytes)   |  Transaction ID (0 for non-tx)
+------------------+
| Type (1 byte)    |  BEGIN=1, PUT=2, DELETE=3, COMMIT=4, ABORT=5
+------------------+
| Timestamp (8)    |  Operation timestamp
+------------------+
| hasKey (1 byte)  |  Boolean flag
+------------------+
| Key (variable)   |  Serialized key
+------------------+
| hasValue (1)     |  Boolean flag
+------------------+
| Value (variable) |  Serialized value
+------------------+
| CRC32 (4 bytes)  |  Checksum
+------------------+
```

### WAL Write Flow

```java
void writeWALRecord(long txId, byte type, K key, V value) {
    // Serialize record
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    dos.writeLong(txId);
    dos.writeByte(type);
    dos.writeLong(System.currentTimeMillis());
    
    // Write key
    if (key != null) {
        dos.writeBoolean(true);
        writeObject(dos, key);
    } else {
        dos.writeBoolean(false);
    }
    
    // Write value
    if (value != null) {
        dos.writeBoolean(true);
        writeObject(dos, value);
    } else {
        dos.writeBoolean(false);
    }
    
    byte[] record = baos.toByteArray();
    
    // Append to WAL with CRC32
    ByteBuffer buffer = ByteBuffer.allocate(4 + record.length + 4);
    buffer.putInt(record.length);
    buffer.put(record);
    
    CRC32 crc32 = new CRC32();
    crc32.update(record);
    buffer.putInt((int) crc32.getValue());
    
    // Force to disk BEFORE returning (durability)
    walChannel.write(buffer, walChannel.size());
    walChannel.force(true);
}
```

### WAL Recovery

On startup, WAL is replayed to recover uncommitted data:

```java
void recoverWAL() {
    Map<Long, List<WALRecord>> txRecords = new HashMap<>();
    List<WALRecord> nonTxRecords = new ArrayList<>();
    
    // Read all WAL records
    long pos = 0;
    while (pos < walChannel.size()) {
        WALRecord record = readWALRecord(pos);
        if (record == null) break;  // Corrupted or end
        
        if (record.txId == 0) {
            nonTxRecords.add(record);
        } else {
            txRecords.computeIfAbsent(record.txId, k -> new ArrayList<>())
                     .add(record);
        }
        pos += 4 + record.recordLength + 4;
    }
    
    // Replay non-transactional operations
    for (WALRecord record : nonTxRecords) {
        replayRecord(record);
    }
    
    // Replay committed transactions only
    for (List<WALRecord> records : txRecords.values()) {
        boolean committed = records.stream()
            .anyMatch(r -> r.type == WAL_COMMIT);
        boolean aborted = records.stream()
            .anyMatch(r -> r.type == WAL_ABORT);
        
        if (committed && !aborted) {
            for (WALRecord record : records) {
                if (record.type == WAL_PUT || record.type == WAL_DELETE) {
                    replayRecord(record);
                }
            }
        }
        // Uncommitted transactions are discarded
    }
    
    // Clear WAL after successful recovery
    walChannel.truncate(0);
}
```

---

## Transaction Management

### Transaction States

```java
enum TxStatus {
    ACTIVE,      // Transaction in progress
    COMMITTED,   // Transaction committed
    ABORTED      // Transaction aborted
}
```

### Transaction Flow

```java
// Begin transaction
Transaction<K, V> beginTx() {
    long txId = txIdGenerator.getAndIncrement();
    Transaction<K, V> tx = new Transaction<>(txId, this);
    activeTransactions.put(txId, tx);
    txWriteSets.put(txId, new ConcurrentHashMap<>());
    
    // Write BEGIN record to WAL
    writeWALRecord(txId, WAL_BEGIN, null, null);
    return tx;
}

// Transaction put (buffered in write set)
void putInternal(Transaction<K, V> tx, K key, V value) {
    writeWALRecord(tx.getTxId(), WAL_PUT, key, value);
    Map<K, V> writeSet = txWriteSets.get(tx.getTxId());
    writeSet.put(key, value);  // Buffer, don't apply yet
}

// Transaction commit
void commitInternal(Transaction<K, V> tx) {
    // Write COMMIT record FIRST (durability)
    writeWALRecord(tx.getTxId(), WAL_COMMIT, null, null);
    
    // Apply write set to memtable
    Map<K, V> writeSet = txWriteSets.remove(tx.getTxId());
    for (Map.Entry<K, V> e : writeSet.entrySet()) {
        if (e.getValue() != null) {
            memTable.put(e.getKey(), e.getValue());
        } else {
            memTable.remove(e.getKey());
        }
    }
    
    activeTransactions.remove(tx.getTxId());
    flushWAL();
}

// Transaction abort
void abortInternal(Transaction<K, V> tx) {
    writeWALRecord(tx.getTxId(), WAL_ABORT, null, null);
    txWriteSets.remove(tx.getTxId());  // Discard write set
    activeTransactions.remove(tx.getTxId());
    flushWAL();
}
```

### Write Set Isolation

Transactions buffer changes in a write set:

```java
V getInternal(Transaction<K, V> tx, K key) {
    // Check transaction's write set first
    Map<K, V> writeSet = txWriteSets.get(tx.getTxId());
    if (writeSet != null && writeSet.containsKey(key)) {
        return writeSet.get(key);
    }
    // Fall back to main store
    return get(key);
}
```

---

## MemTable

### Structure

MemTable is a `ConcurrentHashMap<K, V>` that serves as:

1. **Write buffer**: Accumulates writes before flushing to B+Tree
2. **Read cache**: Most recent writes are served from memtable
3. **MVCC storage**: Transaction write sets are applied here on commit

### Auto-Flush

```java
void put(K key, V value) {
    writeWALRecord(0, WAL_PUT, key, value);
    
    // ConcurrentHashMap doesn't allow null
    if (value == null) {
        memTable.remove(key);
    } else {
        memTable.put(key, value);
    }
    
    // Auto-flush when threshold reached
    if (memTable.size() >= MEMTABLE_FLUSH_THRESHOLD) {  // Default: 500
        flushMemtable();
    }
}
```

### Flush to B+Tree

```java
void flushMemtable() {
    if (memTable.isEmpty()) return;
    
    // Sort entries by key
    List<Entry<K, V>> entries = new ArrayList<>();
    for (Map.Entry<K, V> e : memTable.entrySet()) {
        entries.add(new Entry<>(e.getKey(), e.getValue()));
    }
    entries.sort(Entry.KEY_COMPARATOR);
    
    // Build balanced B+Tree from sorted entries
    BTreeNode<K, V> newTree = buildTree(entries, 0, entries.size());
    this.root = newTree;
    
    // Persist root pointer
    writeHeader();
    
    // Clear memtable
    memTable.clear();
}
```

---

## Compaction

### When Compaction Runs

1. **Manual**: `compactor().compact()`
2. **Automatic**: When fragmentation exceeds threshold (default 30%)
3. **Scheduled**: Periodic check every 30 seconds (if autoCompact enabled)

### Fragmentation Calculation

```java
int estimateFragmentation() {
    long totalSize = store.size();
    if (totalSize == 0) return 0;
    
    long fileSize = store.getFileSize();
    long expectedSize = totalSize * 64;  // Estimate ~64 bytes per entry
    
    if (expectedSize == 0) return 0;
    
    // Fragmentation = excess size / actual size
    long excessSize = fileSize - expectedSize;
    if (excessSize <= 0) return 0;
    
    return (int) Math.min(100, (excessSize * 100) / fileSize);
}
```

### Compaction Process

```java
CompactionStats compact() {
    Instant startTime = Instant.now();
    
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
    
    return new CompactionStats(
        startTime, Instant.now(),
        entriesBefore, entriesAfter,
        sizeBefore, sizeAfter,
        fragmentationBefore, fragmentationAfter
    );
}
```

### Auto-Compaction Scheduler

```java
public Compactor(KVStore store, double threshold, boolean autoCompact) {
    this.store = store;
    this.fragmentationThreshold = threshold;
    this.autoCompact = autoCompact;
    
    if (autoCompact) {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "couchkv-compactor");
            t.setDaemon(true);
            return t;
        });
        
        // Check every 30 seconds
        scheduler.scheduleAtFixedRate(
            this::tryAutoCompact,
            30, 30, TimeUnit.SECONDS
        );
    }
}

void tryAutoCompact() {
    if (compacting || scheduledCompactionRunning.get()) {
        return;
    }
    
    if (needsCompaction()) {
        if (scheduledCompactionRunning.compareAndSet(false, true)) {
            try {
                compact();
            } finally {
                scheduledCompactionRunning.set(false);
            }
        }
    }
}
```

---

## Data Integrity

### CRC32 Checksums

Every block and WAL record includes a CRC32 checksum:

```java
// Block checksum
CRC32 crc32 = new CRC32();
crc32.update(type);
crc32.update(seqBytes);
crc32.update(data);
int checksum = (int) crc32.getValue();

// Verification on read
if (storedCrc != computedCrc) {
    throw new CorruptionException("CRC mismatch");
}
```

### Corruption Handling

Corrupted blocks are detected and skipped during recovery:

```java
WALRecord readWALRecord(long pos) {
    try {
        // Read and verify CRC
        if (storedCrc != computedCrc) {
            return null;  // Corrupted, stop recovery
        }
        
        // Parse record...
        return record;
    } catch (Exception e) {
        return null;  // Any error = corrupted
    }
}
```

---

## Concurrency Control

### Read-Write Lock

```java
private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

V get(K key) {
    lock.readLock().lock();
    try {
        return memTable.get(key) != null 
            ? memTable.get(key) 
            : getFromTree(key);
    } finally {
        lock.readLock().unlock();
    }
}

void put(K key, V value) {
    lock.writeLock().lock();
    try {
        writeWALRecord(0, WAL_PUT, key, value);
        memTable.put(key, value);
    } finally {
        lock.writeLock().unlock();
    }
}
```

### Thread-Safe Collections

- **MemTable**: `ConcurrentHashMap<K, V>`
- **Active Transactions**: `ConcurrentHashMap<Long, Transaction>`
- **Write Sets**: `ConcurrentHashMap<Long, Map<K, V>>`

---

## File Format

### Data File Structure

```
+------------------+
| Header (4KB)     |
+------------------+
| Block 1          |
+------------------+
| Block 2          |
+------------------+
| ...              |
+------------------+
```

### Header Format (4KB)

```
+------------------+
| Magic (8 bytes)  |  "COUCHKV1" = 0x434F5543484B5631
+------------------+
| Version (4)      |  File format version
+------------------+
| Root offset (8)  |  Offset of B+Tree root
+------------------+
| File size (8)    |  Total file size
+------------------+
| Seq num (8)      |  Current sequence number
+------------------+
| Timestamp (8)    |  Last modification time
+------------------+
| CRC32 (4)        |  Header checksum
+------------------+
| Padding          |  Pad to 4KB
+------------------+
```

### WAL File Structure

```
+------------------+
| Record 1         |
+------------------+
| Record 2         |
+------------------+
| ...              |
+------------------+
```

Each WAL record:
- Length-prefixed
- CRC32 protected
- Append-only

---

## Performance Optimizations

### MemTable Buffering

- Writes go to memtable first (fast)
- B+Tree flush on threshold (batched)
- Reduces disk I/O

### WAL Batching

- Multiple operations in single WAL write
- Reduced fsync overhead

### Read Path

1. Check memtable first (O(1))
2. Fall back to B+Tree (O(log n))

### Write Path

1. Write WAL (durability)
2. Update memtable (fast)
3. Async B+Tree flush

---

## Limitations

1. **In-memory B+Tree**: Tree is rebuilt on startup from memtable flushes
2. **No page caching**: All reads go through file I/O
3. **No bloom filters**: All lookups require tree traversal
4. **Limited concurrency**: Single write lock
5. **No compression**: Data stored uncompressed

---

## Future Improvements

1. **Persistent B+Tree**: Store tree structure on disk
2. **Block cache**: LRU cache for hot pages
3. **Bloom filters**: Fast negative lookups
4. **Data compression**: Reduce storage size
5. **Fine-grained locking**: Better concurrency
6. **Snapshot isolation**: True MVCC for reads
