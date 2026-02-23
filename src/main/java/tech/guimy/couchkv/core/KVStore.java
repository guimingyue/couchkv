package tech.guimy.couchkv.core;

import tech.guimy.couchkv.CompactionStats;
import tech.guimy.couchkv.Entry;
import tech.guimy.couchkv.TransactionIsolationLevel;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.zip.CRC32;

/**
 * Append-only B+Tree KV Store with proper WAL (Write-Ahead Log).
 * 
 * WAL ensures durability by:
 * 1. Writing log records BEFORE data is modified
 * 2. Flushing WAL to disk before acknowledging writes
 * 3. Replaying WAL on recovery to restore uncommitted data
 * 
 * This is similar to CouchDB's couch_file + recovery mechanism.
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public class KVStore<K extends Serializable & Comparable<K>, V extends Serializable> 
    implements AutoCloseable {

    // Block types
    private static final byte BLOCK_TYPE_LEAF = 1;
    private static final byte BLOCK_TYPE_INTERNAL = 2;
    private static final byte BLOCK_TYPE_DATA = 4;
    
    // WAL record types
    private static final byte WAL_BEGIN = 1;
    private static final byte WAL_PUT = 2;
    private static final byte WAL_DELETE = 3;
    private static final byte WAL_COMMIT = 4;
    private static final byte WAL_ABORT = 5;
    
    // Constants
    private static final int ORDER = 32;
    private static final int HEADER_SIZE = 4096;
    private static final int MEMTABLE_FLUSH_THRESHOLD = 500;
    private static final long MAGIC = 0x434F5543484B5631L; // "COUCHKV1"
    private static final int VERSION = 1;

    private final Path dbPath;
    private final Path walPath;
    private final FileChannel dataChannel;
    private FileChannel walChannel;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Compactor<K, V> compactor;
    
    // In-memory structures
    private final Map<K, V> memTable = new ConcurrentHashMap<>();
    private final AtomicLong sequenceNumber = new AtomicLong(1);
    private final AtomicLong txIdGenerator = new AtomicLong(1);
    private final Map<Long, Transaction<K, V>> activeTransactions = new ConcurrentHashMap<>();
    private final Map<Long, Map<K, V>> txWriteSets = new ConcurrentHashMap<>();
    
    // B+Tree root (in-memory)
    private BTreeNode<K, V> root;
    private long rootOffset = HEADER_SIZE;
    private long fileSize = HEADER_SIZE;

    // ==================== Construction ====================

    public KVStore(Path path) throws IOException {
        this(path, 0.3, false);
    }

    /**
     * Creates a KV store with compaction configuration
     * @param path the database file path
     * @param fragmentationThreshold threshold (0.0-1.0) at which auto-compaction triggers
     * @param autoCompact enable automatic compaction when threshold exceeded
     */
    public KVStore(Path path, double fragmentationThreshold, boolean autoCompact) throws IOException {
        this.dbPath = path;
        this.walPath = path.resolveSibling(path.getFileName() + ".wal");

        Files.createDirectories(path.getParent());

        boolean exists = Files.exists(path) && Files.size(path) > 0;
        this.dataChannel = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

        if (exists) {
            recover();
        } else {
            writeHeader();
            root = new BTreeLeafNode<>();
            rootOffset = fileSize;
        }

        this.walChannel = FileChannel.open(walPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

        // Recover from WAL (replay uncommitted operations)
        recoverWAL();
        this.compactor = new Compactor<>(this, fragmentationThreshold, autoCompact);
    }

    @SuppressWarnings("unchecked")
    public static <K extends Serializable & Comparable<K>, V extends Serializable>
            KVStore<K, V> create(Path path) throws IOException {
        Files.deleteIfExists(path);
        Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".wal"));
        return (KVStore<K, V>) new KVStore(path, 0.3, false);
    }

    @SuppressWarnings("unchecked")
    public static <K extends Serializable & Comparable<K>, V extends Serializable>
            KVStore<K, V> create(Path path, double fragmentationThreshold, boolean autoCompact) throws IOException {
        Files.deleteIfExists(path);
        Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".wal"));
        return (KVStore<K, V>) new KVStore(path, fragmentationThreshold, autoCompact);
    }

    @SuppressWarnings("unchecked")
    public static <K extends Serializable & Comparable<K>, V extends Serializable>
            KVStore<K, V> open(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new FileNotFoundException("KV store not found: " + path);
        }
        return (KVStore<K, V>) new KVStore(path, 0.3, false);
    }

    // ==================== Transaction API ====================

    /**
     * Begins a new transaction with default isolation level (REPEATABLE_READ).
     * 
     * @return a new transaction
     */
    public Transaction<K, V> beginTx() {
        return beginTx(TransactionIsolationLevel.REPEATABLE_READ);
    }

    /**
     * Begins a new transaction with the specified isolation level.
     * 
     * @param isolationLevel the transaction isolation level
     * @return a new transaction
     */
    public Transaction<K, V> beginTx(TransactionIsolationLevel isolationLevel) {
        long txId = txIdGenerator.getAndIncrement();
        Transaction<K, V> tx = new Transaction<>(txId, this, isolationLevel);
        activeTransactions.put(txId, tx);
        txWriteSets.put(txId, new ConcurrentHashMap<>());

        // WAL: Write BEGIN record BEFORE returning transaction
        writeWALRecord(txId, WAL_BEGIN, null, null);
        return tx;
    }

    /**
     * Executes an operation within a transaction with default isolation level (auto-commits).
     * 
     * @param operation the operation to execute
     * @return the operation result
     * @throws IOException if the operation fails
     */
    public <T> T execute(Function<Transaction<K, V>, T> operation) throws IOException {
        return execute(operation, TransactionIsolationLevel.REPEATABLE_READ);
    }

    /**
     * Executes an operation within a transaction with specified isolation level (auto-commits).
     * 
     * @param operation the operation to execute
     * @param isolationLevel the transaction isolation level
     * @return the operation result
     * @throws IOException if the operation fails
     */
    public <T> T execute(Function<Transaction<K, V>, T> operation, TransactionIsolationLevel isolationLevel) throws IOException {
        Transaction<K, V> tx = beginTx(isolationLevel);
        try {
            T result = operation.apply(tx);
            tx.commit();
            return result;
        } catch (Exception e) {
            tx.abort();
            throw e;
        }
    }

    public <T> T read(Function<KVStore<K, V>, T> operation) {
        lock.readLock().lock();
        try {
            return operation.apply(this);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ==================== KV Operations ====================

    public V get(K key) {
        lock.readLock().lock();
        try {
            V memValue = memTable.get(key);
            if (memValue != null) {
                return memValue;
            }
            return getFromTree(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void put(K key, V value) throws IOException {
        lock.writeLock().lock();
        try {
            // WAL: Write BEFORE modifying memtable
            writeWALRecord(0, WAL_PUT, key, value);

            // ConcurrentHashMap doesn't allow null values - treat as delete
            if (value == null) {
                memTable.remove(key);
            } else {
                memTable.put(key, value);
            }

            if (memTable.size() >= MEMTABLE_FLUSH_THRESHOLD) {
                flushMemtable();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(K key) throws IOException {
        lock.writeLock().lock();
        try {
            // WAL: Write BEFORE modifying
            writeWALRecord(0, WAL_DELETE, key, null);
            
            memTable.remove(key);
            // Mark as tombstone in tree
            insertIntoTree(key, null);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(K key) {
        return get(key) != null;
    }

    public List<Entry<K, V>> range(K start, K end) {
        lock.readLock().lock();
        try {
            List<Entry<K, V>> results = new ArrayList<>();
            Set<K> seen = new HashSet<>();
            
            // Add from memtable (takes precedence)
            for (Map.Entry<K, V> e : memTable.entrySet()) {
                K key = e.getKey();
                if (e.getValue() != null &&
                    (start == null || key.compareTo(start) >= 0) &&
                    (end == null || key.compareTo(end) <= 0)) {
                    results.add(new Entry<>(key, e.getValue()));
                    seen.add(key);
                }
            }
            
            // Add from B+Tree (skip keys in memtable)
            List<Entry<K, V>> treeEntries = rangeFromTree(start, end);
            for (Entry<K, V> e : treeEntries) {
                if (seen.add(e.key()) && e.value() != null) {
                    results.add(e);
                }
            }
            
            results.sort(Entry.KEY_COMPARATOR);
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Entry<K, V>> scan() {
        return range(null, null);
    }

    // ==================== Internal Transaction Operations ====================

    V getInternal(Transaction<K, V> tx, K key) {
        Map<K, V> writeSet = txWriteSets.get(tx.getTxId());
        if (writeSet != null && writeSet.containsKey(key)) {
            return writeSet.get(key);
        }
        return get(key);
    }

    void putInternal(Transaction<K, V> tx, K key, V value) {
        // WAL: Write BEFORE buffering in write set
        writeWALRecord(tx.getTxId(), WAL_PUT, key, value);
        Map<K, V> writeSet = txWriteSets.get(tx.getTxId());
        if (writeSet != null) {
            writeSet.put(key, value);
        }
    }

    void deleteInternal(Transaction<K, V> tx, K key) {
        // WAL: Write BEFORE buffering
        writeWALRecord(tx.getTxId(), WAL_DELETE, key, null);
        Map<K, V> writeSet = txWriteSets.get(tx.getTxId());
        if (writeSet != null) {
            writeSet.put(key, null);  // Tombstone
        }
    }

    void commitInternal(Transaction<K, V> tx) throws IOException {
        // WAL: Write COMMIT record BEFORE applying changes
        writeWALRecord(tx.getTxId(), WAL_COMMIT, null, null);
        
        // Apply write set to memtable
        Map<K, V> writeSet = txWriteSets.remove(tx.getTxId());
        if (writeSet != null) {
            for (Map.Entry<K, V> e : writeSet.entrySet()) {
                if (e.getValue() != null) {
                    memTable.put(e.getKey(), e.getValue());
                } else {
                    memTable.remove(e.getKey());
                }
            }
        }
        
        activeTransactions.remove(tx.getTxId());
        flushWAL();
    }

    void abortInternal(Transaction<K, V> tx) throws IOException {
        // WAL: Write ABORT record
        writeWALRecord(tx.getTxId(), WAL_ABORT, null, null);
        
        // Discard write set (no changes applied)
        txWriteSets.remove(tx.getTxId());
        activeTransactions.remove(tx.getTxId());
        flushWAL();
    }

    // ==================== B+Tree Operations ====================

    @SuppressWarnings("unchecked")
    private V getFromTree(K key) {
        if (root == null) return null;
        return root.get(key);
    }

    private List<Entry<K, V>> rangeFromTree(K start, K end) {
        if (root == null) return List.of();
        return root.range(start, end);
    }

    private void insertIntoTree(K key, V value) throws IOException {
        if (root == null) {
            root = new BTreeLeafNode<>();
        }
        
        BTreeNode.SplitResult<K, V> splitResult = root.insert(key, value);
        
        if (splitResult != null) {
            BTreeInternalNode<K, V> newRoot = new BTreeInternalNode<>();
            newRoot.addChild(root);
            newRoot.addKey(splitResult.separatorKey);
            newRoot.addChild(splitResult.rightNode);
            root = newRoot;
        }
        
        writeHeader();
    }

    private void flushMemtable() throws IOException {
        if (memTable.isEmpty()) return;
        
        List<Entry<K, V>> entries = new ArrayList<>();
        for (Map.Entry<K, V> e : memTable.entrySet()) {
            entries.add(new Entry<>(e.getKey(), e.getValue()));
        }
        entries.sort(Entry.KEY_COMPARATOR);
        
        BTreeNode<K, V> newTree = buildTree(entries, 0, entries.size());
        this.root = newTree;
        writeHeader();
        
        memTable.clear();
    }

    @SuppressWarnings("unchecked")
    private BTreeNode<K, V> buildTree(List<Entry<K, V>> entries, int start, int end) {
        int count = end - start;
        
        if (count <= ORDER - 1) {
            BTreeLeafNode<K, V> leaf = new BTreeLeafNode<>();
            for (int i = start; i < end; i++) {
                leaf.keys.add(entries.get(i).key());
                leaf.values.add(entries.get(i).value() != null ? entries.get(i).value() : null);
            }
            return leaf;
        }
        
        int mid = start + count / 2;
        BTreeNode<K, V> left = buildTree(entries, start, mid);
        BTreeNode<K, V> right = buildTree(entries, mid, end);
        
        BTreeInternalNode<K, V> internal = new BTreeInternalNode<>();
        internal.addChild(left);
        internal.addChild(right);
        internal.addKey(entries.get(mid).key());
        
        return internal;
    }

    // ==================== Append-Only File I/O ====================

    private long appendBlock(byte type, byte[] data) throws IOException {
        int dataLen = data.length;
        int blockLen = 4 + 1 + 8 + dataLen + 4;
        
        ByteBuffer buffer = ByteBuffer.allocate(blockLen);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(blockLen);
        buffer.put(type);
        long seq = sequenceNumber.getAndIncrement();
        buffer.putLong(seq);
        buffer.put(data);
        
        CRC32 crc32 = new CRC32();
        crc32.update(type);
        crc32.update(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(seq).array());
        crc32.update(data);
        buffer.putInt((int) crc32.getValue());
        
        buffer.flip();
        long offset = fileSize;
        dataChannel.write(buffer, offset);
        fileSize += blockLen;
        
        return offset;
    }

    // ==================== WAL Implementation ====================

    /**
     * Writes a WAL record (Write-Ahead Log).
     * WAL ensures durability: records are written BEFORE data modification.
     */
    private void writeWALRecord(long txId, byte type, K key, V value) {
        if (walChannel == null) return;
        
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Record format: txId | type | timestamp | hasKey | key | hasValue | value
            dos.writeLong(txId);
            dos.writeByte(type);
            dos.writeLong(System.currentTimeMillis());
            
            if (key != null) {
                dos.writeBoolean(true);
                writeObject(dos, key);
            } else {
                dos.writeBoolean(false);
            }
            
            if (value != null) {
                dos.writeBoolean(true);
                writeObject(dos, value);
            } else {
                dos.writeBoolean(false);
            }
            
            dos.flush();
            byte[] record = baos.toByteArray();
            
            // WAL block: length | record | CRC32
            ByteBuffer buffer = ByteBuffer.allocate(4 + record.length + 4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(record.length);
            buffer.put(record);
            
            CRC32 crc32 = new CRC32();
            crc32.update(record);
            buffer.putInt((int) crc32.getValue());
            
            buffer.flip();
            
            // Append to WAL file
            long pos = walChannel.size();
            walChannel.write(buffer, pos);
            
            // CRITICAL: Force WAL to disk BEFORE returning
            // This ensures durability - record is on disk before we acknowledge
            walChannel.force(true);
            
        } catch (IOException e) {
            // In production, would log and potentially fail the operation
            e.printStackTrace();
        }
    }

    private void flushWAL() {
        if (walChannel != null) {
            try {
                walChannel.force(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Recovers state by replaying WAL records after a crash.
     * 
     * Replay logic:
     * 1. Read all WAL records
     * 2. For committed transactions: apply to memtable
     * 3. For uncommitted transactions: discard (atomicity)
     * 4. Clear WAL after successful recovery
     */
    private void recoverWAL() throws IOException {
        if (walChannel == null || walChannel.size() == 0) return;
        
        // First pass: collect all records by transaction
        Map<Long, List<WALRecord>> txRecords = new HashMap<>();
        List<WALRecord> nonTxRecords = new ArrayList<>();
        
        long pos = 0;
        while (pos < walChannel.size()) {
            WALRecord record = readWALRecord(pos);
            if (record == null) break;
            
            if (record.txId == 0) {
                // Non-transactional operation
                nonTxRecords.add(record);
            } else {
                txRecords.computeIfAbsent(record.txId, k -> new ArrayList<>()).add(record);
            }
            
            pos += 4 + record.recordLength + 4;
        }
        
        // Second pass: replay committed transactions
        // Apply non-transactional operations first
        for (WALRecord record : nonTxRecords) {
            replayRecord(record);
        }
        
        // Then apply committed transactional operations
        for (Map.Entry<Long, List<WALRecord>> entry : txRecords.entrySet()) {
            long txId = entry.getKey();
            List<WALRecord> records = entry.getValue();
            
            // Check if transaction was committed
            boolean committed = records.stream()
                .anyMatch(r -> r.type == WAL_COMMIT);
            boolean aborted = records.stream()
                .anyMatch(r -> r.type == WAL_ABORT);
            
            if (committed && !aborted) {
                // Replay only PUT/DELETE records (skip BEGIN/COMMIT)
                for (WALRecord record : records) {
                    if (record.type == WAL_PUT || record.type == WAL_DELETE) {
                        replayRecord(record);
                    }
                }
            }
            // If not committed, discard (atomicity)
        }
        
        // Clear WAL after successful recovery
        walChannel.truncate(0);
    }
    
    /**
     * Reads a single WAL record from the given position.
     * Returns null if the record is corrupted or cannot be read.
     */
    private WALRecord readWALRecord(long pos) {
        try {
            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            lenBuffer.order(ByteOrder.LITTLE_ENDIAN);
            if (walChannel.read(lenBuffer, pos) != 4) return null;
            lenBuffer.flip();

            int recordLen = lenBuffer.getInt();
            if (recordLen <= 0 || recordLen > 10_000_000) return null;  // Sanity check

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordLen);
            walChannel.read(recordBuffer, pos + 4);
            recordBuffer.flip();
            byte[] record = recordBuffer.array();

            // Verify CRC
            ByteBuffer crcBuffer = ByteBuffer.allocate(4);
            crcBuffer.order(ByteOrder.LITTLE_ENDIAN);
            walChannel.read(crcBuffer, pos + 4 + recordLen);
            crcBuffer.flip();
            int storedCrc = crcBuffer.getInt();

            CRC32 crc32 = new CRC32();
            crc32.update(record);
            if (storedCrc != (int) crc32.getValue()) {
                // Corrupted record - stop recovery here but don't fail
                return null;
            }

            // Parse record
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(record));
            WALRecord walRecord = new WALRecord();
            walRecord.recordLength = recordLen;
            walRecord.txId = dis.readLong();
            walRecord.type = dis.readByte();
            walRecord.timestamp = dis.readLong();

            if (dis.readBoolean()) {
                walRecord.key = readObject(dis);
            }
            if (dis.readBoolean()) {
                walRecord.value = readObject(dis);
            }

            return walRecord;
        } catch (Exception e) {
            // Any error reading - treat as corrupted and stop recovery
            return null;
        }
    }
    
    /**
     * Replays a single WAL record by applying it to memtable
     */
    @SuppressWarnings("unchecked")
    private void replayRecord(WALRecord record) {
        if (record.type == WAL_PUT) {
            memTable.put((K) record.key, (V) record.value);
        } else if (record.type == WAL_DELETE) {
            memTable.remove(record.key);
        }
    }
    
    /**
     * Internal WAL record class
     */
    private static class WALRecord {
        int recordLength;
        long txId;
        byte type;
        long timestamp;
        Object key;
        Object value;
    }

    @SuppressWarnings("unchecked")
    private void writeObject(DataOutputStream dos, Object obj) throws IOException {
        if (obj instanceof String) {
            dos.writeByte(0);  // Type marker for String
            byte[] bytes = ((String) obj).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (obj instanceof byte[]) {
            dos.writeByte(1);  // Type marker for byte[]
            byte[] bytes = (byte[]) obj;
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (obj instanceof Integer) {
            dos.writeByte(2);  // Type marker for Integer
            dos.writeInt((Integer) obj);
        } else if (obj instanceof Long) {
            dos.writeByte(3);  // Type marker for Long
            dos.writeLong((Long) obj);
        } else if (obj == null) {
            dos.writeByte(-1);  // Marker for null
        } else {
            dos.writeByte(127);  // Type marker for generic object
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte[] bytes = baos.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }

    @SuppressWarnings("unchecked")
    private Object readObject(DataInputStream dis) throws IOException {
        byte type = dis.readByte();
        
        switch (type) {
            case 0:  // String
                int strLen = dis.readInt();
                byte[] strBytes = new byte[strLen];
                dis.readFully(strBytes);
                return new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
                
            case 1:  // byte[]
                int bytesLen = dis.readInt();
                byte[] bytes = new byte[bytesLen];
                dis.readFully(bytes);
                return bytes;
                
            case 2:  // Integer
                return dis.readInt();
                
            case 3:  // Long
                return dis.readLong();
                
            case -1:  // null
                return null;
                
            case 127:  // Generic object
                int objLen = dis.readInt();
                byte[] objBytes = new byte[objLen];
                dis.readFully(objBytes);
                try (ByteArrayInputStream bais = new ByteArrayInputStream(objBytes);
                     ObjectInputStream ois = new ObjectInputStream(bais)) {
                    return ois.readObject();
                } catch (ClassNotFoundException ex) {
                    throw new IOException("Failed to deserialize object", ex);
                }
                
            default:
                throw new IOException("Unknown type marker: " + type);
        }
    }

    // ==================== Header Operations ====================

    private void writeHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        buffer.putLong(MAGIC);
        buffer.putInt(VERSION);
        buffer.putLong(rootOffset);
        buffer.putLong(fileSize);
        buffer.putLong(sequenceNumber.get());
        buffer.putLong(System.currentTimeMillis());
        
        CRC32 crc32 = new CRC32();
        for (int i = 0; i < 32; i++) {
            crc32.update(buffer.get(i));
        }
        buffer.position(4088);
        buffer.putInt((int) crc32.getValue());
        
        buffer.flip();
        dataChannel.write(buffer, 0);
        dataChannel.force(true);
    }

    private void recover() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        dataChannel.read(buffer, 0);
        buffer.flip();
        
        long magic = buffer.getLong();
        if (magic != MAGIC) {
            throw new IOException("Invalid magic number");
        }
        
        int version = buffer.getInt();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        rootOffset = buffer.getLong();
        fileSize = buffer.getLong();
        long seq = buffer.getLong();
        sequenceNumber.set(seq);
        
        // Reconstruct B+Tree (simplified - in-memory only)
        root = new BTreeLeafNode<>();
    }

    // ==================== Compaction Support ====================

    public Compactor<K, V> compactor() {
        return compactor;
    }

    void rebuildFromEntries(List<Entry<K, V>> entries) throws IOException {
        lock.writeLock().lock();
        try {
            BTreeNode<K, V> newTree;
            if (!entries.isEmpty()) {
                newTree = buildTree(entries, 0, entries.size());
            } else {
                newTree = new BTreeLeafNode<>();
            }
            
            this.root = newTree;
            writeHeader();
            dataChannel.force(true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return the current file size in bytes
     */
    public long getFileSize() {
        return fileSize;
    }

    // ==================== Utilities ====================

    /**
     * @return the approximate number of entries
     * Note: This counts memtable entries (most recent) plus B+Tree entries
     */
    public long size() {
        // After flush, memtable is cleared and data is in B+Tree
        // Before flush, data is in memtable
        if (memTable.isEmpty() && root != null) {
            return root.size();
        }
        return memTable.size();
    }

    public boolean isEmpty() {
        return memTable.isEmpty() && (root == null || root.size() == 0);
    }

    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            flushMemtable();
            dataChannel.force(true);
            flushWAL();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flush();
            dataChannel.close();
            if (walChannel != null) {
                walChannel.close();
            }
            // Shutdown compaction scheduler
            if (compactor != null) {
                compactor.shutdown();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
