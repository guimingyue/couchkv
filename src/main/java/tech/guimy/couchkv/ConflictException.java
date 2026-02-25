package tech.guimy.couchkv;

/**
 * Exception thrown when a document update conflict occurs.
 * 
 * This happens when:
 * - The provided revision doesn't match the current revision
 * - Two concurrent updates try to modify the same document
 * 
 * Similar to CouchDB's 409 Conflict response.
 */
public class ConflictException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    private final String documentId;
    private final String expectedRev;
    private final String actualRev;
    
    public ConflictException(String documentId, String expectedRev, String actualRev) {
        super(String.format("Document update conflict for '%s': expected rev '%s' but found '%s'",
            documentId, expectedRev, actualRev));
        this.documentId = documentId;
        this.expectedRev = expectedRev;
        this.actualRev = actualRev;
    }
    
    public ConflictException(String documentId) {
        super(String.format("Document update conflict for '%s'", documentId));
        this.documentId = documentId;
        this.expectedRev = null;
        this.actualRev = null;
    }
    
    public ConflictException(String message, Throwable cause) {
        super(message, cause);
        this.documentId = null;
        this.expectedRev = null;
        this.actualRev = null;
    }
    
    public String getDocumentId() {
        return documentId;
    }
    
    public String getExpectedRev() {
        return expectedRev;
    }
    
    public String getActualRev() {
        return actualRev;
    }
}