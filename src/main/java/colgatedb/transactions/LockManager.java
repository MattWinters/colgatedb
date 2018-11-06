package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.List;

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
public interface LockManager {

    /**
     * Attempts to acquire a lock on a particular pid on behalf of a given transaction.  Updates
     * state of the lock manager as appropriate. The thread calling this method will wait until
     * lock is acquired.
     *
     * The rules governing when a lock can be granted are outlined in the textbook (Ch. 17).
     *
     * @param tid txn requesting the lock
     * @param pid id of the page on which lock is desired
     * @param perm determines whether the lock is shared (read only) or exclusive (read write)
     * @throws TransactionAbortedException if deadlock is detected (or a timeout is reached).
     */
    void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException;

    /**
     * Indicates whether a page with given pid is currently locked by given tid with permissions
     * AT LEAST AS STRONG as the ones specified.
     *
     * For example, if the transaction has an exclusive lock (read-write) and this method is
     * called with perm equal to {@link Permissions#READ_ONLY} then it will return true.
     * @param tid transaction id
     * @param pid page id
     * @param perm permissions
     * @return true if tid holds a lock on pid with permissions at least as strong as perm,
     *         false otherwise
     */
    boolean holdsLock(TransactionId tid, PageId pid, Permissions perm);

    /**
     * Release the lock held by transaction tid on page with given pid and notify any waiting
     * threads.
     *
     * @param tid transaction id
     * @param pid page id
     * @throws LockManagerException if tid does not hold lock on this pid
     */
    void releaseLock(TransactionId tid, PageId pid) throws LockManagerException;

    /**
     * @param tid transaction id
     * @return a list of all of the page ids on which this transaction currently has locks
     */
    List<PageId> getPagesForTid(TransactionId tid);

    /**
     * @param pid page id
     * @return a list of the transaction ids of the transactions holding the lock on given pid
     */
    List<TransactionId> getTidsForPage(PageId pid);
}
