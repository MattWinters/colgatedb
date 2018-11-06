package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.*;

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
public class LockManagerImpl implements LockManager {


    public LockManagerImpl() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }
}
