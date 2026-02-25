package tech.guimy.couchkv;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a document with revision tracking, similar to CouchDB's document model.
 * 
 * Each document has:
 * - A unique ID (key)
 * - A revision string (_rev) for optimistic concurrency control
 * - The actual value
 * - A sequence number for MVCC and change tracking
 * 
 * The revision format is: "rev-seq" where rev is an incrementing number
 * and seq is a hash of the content. Example: "1-969a00d5", "2-3b6e9f2a"
 */
public final class Document<V extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String id;
    private final String rev;
    private final V value;
    private final long sequenceNumber;
    private final boolean deleted;
    
    public Document(String id, V value) {
        this(id, "1-" + generateRevHash(value), value, 0, false);
    }
    
    public Document(String id, String rev, V value, long sequenceNumber, boolean deleted) {
        this.id = id;
        this.rev = rev;
        this.value = value;
        this.sequenceNumber = sequenceNumber;
        this.deleted = deleted;
    }
    
    public String getId() {
        return id;
    }
    
    public String getRev() {
        return rev;
    }
    
    public V getValue() {
        return value;
    }
    
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    public boolean isDeleted() {
        return deleted;
    }
    
    /**
     * Creates a new revision of this document with an updated value.
     * Increments the revision number.
     */
    public Document<V> update(V newValue, long newSeqNum) {
        int revNum = getRevisionNumber();
        String newRev = (revNum + 1) + "-" + generateRevHash(newValue);
        return new Document<>(id, newRev, newValue, newSeqNum, false);
    }
    
    /**
     * Creates a tombstone revision (deleted document).
     */
    public Document<V> delete(long newSeqNum) {
        int revNum = getRevisionNumber();
        String newRev = (revNum + 1) + "-" + generateRevHash(null);
        return new Document<>(id, newRev, null, newSeqNum, true);
    }
    
    /**
     * Extracts the revision number from the revision string.
     */
    public int getRevisionNumber() {
        if (rev == null || rev.isEmpty()) {
            return 0;
        }
        int dashIndex = rev.indexOf('-');
        if (dashIndex > 0) {
            try {
                return Integer.parseInt(rev.substring(0, dashIndex));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }
    
    /**
     * Generates a hash for revision identification.
     */
    private static String generateRevHash(Object value) {
        int hash = value == null ? 0 : value.hashCode();
        return String.format("%08x", hash);
    }
    
    /**
     * Parses a revision string to get the revision number.
     */
    public static int parseRevisionNumber(String rev) {
        if (rev == null || rev.isEmpty()) {
            return 0;
        }
        int dashIndex = rev.indexOf('-');
        if (dashIndex > 0) {
            try {
                return Integer.parseInt(rev.substring(0, dashIndex));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }
    
    /**
     * Creates a new document with initial revision.
     */
    public static <V extends Serializable> Document<V> create(String id, V value, long seqNum) {
        return new Document<>(id, "1-" + generateRevHash(value), value, seqNum, false);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document<?> document = (Document<?>) o;
        return Objects.equals(id, document.id) && 
               Objects.equals(rev, document.rev);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, rev);
    }
    
    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", rev='" + rev + '\'' +
                ", deleted=" + deleted +
                '}';
    }
}