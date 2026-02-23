package tech.guimy.couchkv;

/**
 * Transaction isolation levels.
 * 
 * Defines the visibility of changes made by one transaction to other concurrent transactions.
 * 
 * @see <a href="https://en.wikipedia.org/wiki/Isolation_(database_systems)">Isolation (database systems)</a>
 */
public enum TransactionIsolationLevel {

    /**
     * Dirty reads, non-repeatable reads, and phantom reads are possible.
     * 
     * A transaction can read uncommitted changes made by other transactions.
     * This is the lowest isolation level.
     */
    READ_UNCOMMITTED,

    /**
     * Dirty reads are prevented, but non-repeatable reads and phantom reads are possible.
     * 
     * A transaction can only read committed changes made by other transactions.
     * However, if the transaction reads the same row twice, it may get different values
     * if another transaction modified the row in between.
     */
    READ_COMMITTED,

    /**
     * Dirty reads and non-repeatable reads are prevented, but phantom reads are possible.
     * 
     * A transaction will see a consistent snapshot of the database at the start of the transaction.
     * If the transaction reads the same row twice, it will get the same value both times.
     * However, new rows inserted by other transactions may appear in subsequent reads.
     * 
     * This is the default isolation level for CouchKV.
     */
    REPEATABLE_READ,

    /**
     * Dirty reads, non-repeatable reads, and phantom reads are prevented.
     * 
     * A transaction will see a completely isolated view of the database.
     * No changes made by other transactions are visible until the transaction completes.
     * This is the highest isolation level but may have performance implications.
     */
    SERIALIZABLE
}
