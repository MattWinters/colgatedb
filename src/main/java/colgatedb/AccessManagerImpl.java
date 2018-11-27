package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.transactions.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used

    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void allocatePage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public void transactionComplete(TransactionId tid, boolean commit) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void setForce(boolean force) {
        // you do NOT need to implement this for lab10.  this will be changed in a later lab.
        throw new UnsupportedOperationException("implement me!");
    }
}
