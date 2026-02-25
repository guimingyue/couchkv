package tech.guimy.couchkv;

import tech.guimy.couchkv.core.KVStore;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A document store with revision tracking, similar to CouchDB's document API.
 * 
 * Provides:
 * - Optimistic concurrency control via revision tracking (_rev)
 * - Document creation, update, and deletion with conflict detection
 * - Sequence numbers for change tracking
 * 
 * @param <V> the document value type
 */
public class DocumentStore<V extends Serializable> implements AutoCloseable {
    
    private final KVStore<String, Document<V>> store;
    
    public DocumentStore(Path path) throws IOException {
        this.store = KVStore.create(path);
    }
    
    public DocumentStore(Path path, double fragmentationThreshold, boolean autoCompact) throws IOException {
        this.store = KVStore.create(path, fragmentationThreshold, autoCompact);
    }
    
    @SuppressWarnings("unchecked")
    public static <V extends Serializable> DocumentStore<V> open(Path path) throws IOException {
        KVStore<String, Document<V>> kvStore = KVStore.open(path);
        return new DocumentStore<>(kvStore);
    }
    
    private DocumentStore(KVStore<String, Document<V>> store) {
        this.store = store;
    }
    
    /**
     * Creates a new document with an automatically generated revision.
     * 
     * @param id the document ID
     * @param value the document value
     * @return the created document with revision
     * @throws ConflictException if a document with the same ID already exists
     */
    public Document<V> create(String id, V value) throws IOException, ConflictException {
        Document<V> existing = store.get(id);
        if (existing != null && !existing.isDeleted()) {
            throw new ConflictException(id);
        }
        
        long seqNum = store.getSequenceNumber();
        Document<V> doc = Document.create(id, value, seqNum + 1);
        store.put(id, doc);
        return doc;
    }
    
    /**
     * Reads a document by ID.
     * 
     * @param id the document ID
     * @return the document, or empty if not found
     */
    public Optional<Document<V>> read(String id) {
        Document<V> doc = store.get(id);
        if (doc == null || doc.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(doc);
    }
    
    /**
     * Updates a document with optimistic concurrency control.
     * 
     * @param id the document ID
     * @param expectedRev the expected current revision
     * @param newValue the new value
     * @return the updated document with new revision
     * @throws ConflictException if the expected revision doesn't match
     */
    public Document<V> update(String id, String expectedRev, V newValue) throws IOException, ConflictException {
        Document<V> existing = store.get(id);
        
        if (existing == null) {
            throw new ConflictException(id, expectedRev, null);
        }
        
        if (!existing.getRev().equals(expectedRev)) {
            throw new ConflictException(id, expectedRev, existing.getRev());
        }
        
        long seqNum = store.getSequenceNumber();
        Document<V> updated = existing.update(newValue, seqNum + 1);
        store.put(id, updated);
        return updated;
    }
    
    /**
     * Updates a document without revision check (unsafe).
     * 
     * @param id the document ID
     * @param newValue the new value
     * @return the updated document with new revision
     */
    public Document<V> updateUnsafe(String id, V newValue) throws IOException {
        Document<V> existing = store.get(id);
        
        if (existing == null) {
            return create(id, newValue);
        }
        
        long seqNum = store.getSequenceNumber();
        Document<V> updated = existing.update(newValue, seqNum + 1);
        store.put(id, updated);
        return updated;
    }
    
    /**
     * Deletes a document with optimistic concurrency control.
     * 
     * @param id the document ID
     * @param expectedRev the expected current revision
     * @throws ConflictException if the expected revision doesn't match
     */
    public void delete(String id, String expectedRev) throws IOException, ConflictException {
        Document<V> existing = store.get(id);
        
        if (existing == null) {
            throw new ConflictException(id, expectedRev, null);
        }
        
        if (!existing.getRev().equals(expectedRev)) {
            throw new ConflictException(id, expectedRev, existing.getRev());
        }
        
        long seqNum = store.getSequenceNumber();
        Document<V> tombstone = existing.delete(seqNum + 1);
        store.put(id, tombstone);
    }
    
    /**
     * Deletes a document without revision check (unsafe).
     * 
     * @param id the document ID
     */
    public void deleteUnsafe(String id) throws IOException {
        Document<V> existing = store.get(id);
        
        if (existing == null) {
            long seqNum = store.getSequenceNumber();
            Document<V> tombstone = new Document<>(id, "1-" + Document.parseRevisionNumber(null), null, seqNum + 1, true);
            store.put(id, tombstone);
            return;
        }
        
        long seqNum = store.getSequenceNumber();
        Document<V> tombstone = existing.delete(seqNum + 1);
        store.put(id, tombstone);
    }
    
    /**
     * Checks if a document exists.
     * 
     * @param id the document ID
     * @return true if the document exists and is not deleted
     */
    public boolean exists(String id) {
        Document<V> doc = store.get(id);
        return doc != null && !doc.isDeleted();
    }
    
    /**
     * Gets the current revision of a document.
     * 
     * @param id the document ID
     * @return the revision string, or null if not found
     */
    public String getRevision(String id) {
        Document<V> doc = store.get(id);
        return doc != null ? doc.getRev() : null;
    }
    
    /**
     * Lists all document IDs.
     * 
     * @return list of all document IDs (excluding deleted)
     */
    public List<String> listIds() {
        List<String> ids = new ArrayList<>();
        for (Entry<String, Document<V>> entry : store.scan()) {
            if (entry.value() != null && !entry.value().isDeleted()) {
                ids.add(entry.key());
            }
        }
        return ids;
    }
    
    /**
     * Gets the number of documents (excluding deleted).
     * 
     * @return document count
     */
    public long count() {
        long count = 0;
        for (Entry<String, Document<V>> entry : store.scan()) {
            if (entry.value() != null && !entry.value().isDeleted()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Flushes any pending data to disk.
     */
    public void flush() throws IOException {
        store.flush();
    }
    
    /**
     * Gets the underlying KV store for advanced operations.
     */
    public KVStore<String, Document<V>> getKVStore() {
        return store;
    }
    
    @Override
    public void close() throws IOException {
        store.close();
    }
}