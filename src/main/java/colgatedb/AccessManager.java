package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public interface AccessManager {

    /**
     * @see colgatedb.transactions.LockManager#acquireLock(TransactionId, PageId, Permissions)
     */
    void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException;

    /**
     * @see colgatedb.transactions.LockManager#holdsLock(TransactionId, PageId, Permissions)
     */
    boolean holdsLock(TransactionId tid, PageId pid, Permissions perm);

    /**
     * @see colgatedb.transactions.LockManager#releaseLock(TransactionId, PageId)
     */
    void releaseLock(TransactionId tid, PageId pid);

    /**
     * Pins the page and keeps track of the number of times each transaction has pinned this page.
     * @see BufferManager#pinPage(PageId, PageMaker)
     */
    Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker);

    /**
     * Unpins the page and keeps track of the number of times each transaction has pinned this page.
     * @see BufferManager#unpinPage(PageId, boolean)
     */
    void unpinPage(TransactionId tid, Page page, boolean isDirty);

    /**
     * @see BufferManager#allocatePage(PageId)
     */
    void allocatePage(PageId pid);

    /**
     * Complete the commit of a transaction.
     * @see AccessManager#transactionComplete(TransactionId, boolean)
     * @param tid the ID of the transaction that is completing
     */
    void transactionComplete(TransactionId tid);

    /**
     * Complete a transaction, committing or aborting depending on flag.
     * @param tid the ID of the transaction that is completing
     * @param commit a flag indicating whether we should commit or abort
     */
    void transactionComplete(TransactionId tid, boolean commit);

    /**
     * Set policy regarding dirty pages on commit.  If force is true, pages must
     * be flushed to disk upon commit.  If false, dirty pages can remain in buffer pool.
     * @param force true if force policy is desired, false otherwise
     */
    void setForce(boolean force);
}
