package tech.guimy.couchkv;

/**
 * Transaction status
 */
public enum TxStatus {
    /** Transaction is active */
    ACTIVE,
    /** Transaction has been committed */
    COMMITTED,
    /** Transaction has been aborted */
    ABORTED
}
