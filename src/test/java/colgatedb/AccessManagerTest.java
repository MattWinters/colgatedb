package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

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
public class AccessManagerTest extends AccessManagerTestBase {

    /**
     * Reprise of lock manager test.
     * @see colgatedb.transactions.LockManagerTest
     * @throws TransactionAbortedException
     */
    @Test
    @GradedTest(number="23.1", max_score=1.0, visibility="visible")
    public void holdsLock() throws TransactionAbortedException {
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_WRITE));
        am.acquireLock(tid0, pid0, Permissions.READ_ONLY);
        assertTrue(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_WRITE));
        assertFalse(am.holdsLock(tid0, pid1, Permissions.READ_ONLY));  // make sure lock is specific to page
    }

    /**
     * Reprise of lock manager test.
     * @see colgatedb.transactions.LockManagerTest
     * @throws TransactionAbortedException
     */
    @Test
    @GradedTest(number="23.2", max_score=1.0, visibility="visible")
    public void holdsLockExclusive() throws TransactionAbortedException {
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_WRITE));
        am.acquireLock(tid0, pid0, Permissions.READ_WRITE);
        assertTrue(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));  // should return true b/c held lock is at least as strong
        assertTrue(am.holdsLock(tid0, pid0, Permissions.READ_WRITE));
    }

    /**
     * Reprise of lock manager test.
     * @see colgatedb.transactions.LockManagerTest
     * @throws TransactionAbortedException
     */
    @Test
    @GradedTest(number="23.3", max_score=1.0, visibility="visible")
    public void releaseLock() throws TransactionAbortedException {
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        am.acquireLock(tid0, pid0, Permissions.READ_ONLY);
        assertTrue(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        am.releaseLock(tid0, pid0);
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
    }

    @Test
    @GradedTest(number="23.4", max_score=1.0, visibility="visible")
    public void testPinningPage() throws TransactionAbortedException {
        am.acquireLock(tid0, pid0, Permissions.READ_ONLY);
        Page page = am.pinPage(tid0, pid0, pm);
        assertEquals(1, bm.pinCount(pid0));
        am.pinPage(tid0, pid0, pm);
        assertEquals(2, bm.pinCount(pid0));
        am.unpinPage(tid0, page, false);
        assertEquals(1, bm.pinCount(pid0));
    }

    @Test
    @GradedTest(number="23.5", max_score=1.0, visibility="visible")
    public void testAllocatePage() throws TransactionAbortedException {
        am.allocatePage(pid0);
        assertTrue(bm.isPageAllocated(pid0));
    }


    @Test
    @GradedTest(number="23.6", max_score=1.0, visibility="visible")
    public void testLocksAreReleasedOnCommit() throws TransactionAbortedException {
        acquireThenComplete(true);
    }

    @Test
    @GradedTest(number="23.7", max_score=1.0, visibility="visible")
    public void testLocksAreReleasedOnAbort() throws TransactionAbortedException {
        acquireThenComplete(false);
    }

    private void acquireThenComplete(boolean commit) throws TransactionAbortedException {
        am.acquireLock(tid0, pid0, Permissions.READ_ONLY);
        am.acquireLock(tid0, pid1, Permissions.READ_WRITE);
        am.transactionComplete(tid0, commit);
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
        assertFalse(am.holdsLock(tid0, pid0, Permissions.READ_ONLY));
    }

    @Test
    @GradedTest(number="23.8", max_score=1.0, visibility="visible")
    public void testDirtyPagesFlushedOnCommit() throws TransactionAbortedException {
        am.setForce(true);
        am.acquireLock(tid0, pid0, Permissions.READ_WRITE);
        am.acquireLock(tid0, pid1, Permissions.READ_WRITE);

        MockPage page0 = (MockPage) am.pinPage(tid0, pid0, pm);
        MockPage page1 = (MockPage) am.pinPage(tid0, pid1, pm);

        am.unpinPage(tid0, page0, true);
        am.unpinPage(tid0, page1, false);

        am.transactionComplete(tid0);

        assertTrue(bm.wasFlushed(pid0));   // pid0 is dirty
        assertFalse(bm.wasFlushed(pid1));  // pid1 isn't dirty
    }

    @Test
    @GradedTest(number="23.9", max_score=1.0, visibility="visible")
    public void testDirtyPagesDiscardedOnAbort() throws TransactionAbortedException {
        am.setForce(true);
        am.acquireLock(tid0, pid0, Permissions.READ_WRITE);
        am.acquireLock(tid0, pid1, Permissions.READ_WRITE);

        MockPage page0 = (MockPage) am.pinPage(tid0, pid0, pm);
        MockPage page1 = (MockPage) am.pinPage(tid0, pid1, pm);

        am.unpinPage(tid0, page0, true);
        am.unpinPage(tid0, page1, false);

        am.transactionComplete(tid0, false); // abort

        assertFalse(bm.inBufferPool(pid0));  // was dirty, should be discarded
        assertTrue(bm.inBufferPool(pid1));   // never dirtied, no need to discard
        assertFalse(bm.wasFlushed(pid0));
        assertFalse(bm.wasFlushed(pid1));
    }

    @Test
    @GradedTest(number="23.10", max_score=1.0, visibility="visible")
    public void testPagesUnpinnedOnAbort() throws TransactionAbortedException {
        am.acquireLock(tid0, pid0, Permissions.READ_WRITE);
        am.acquireLock(tid0, pid1, Permissions.READ_WRITE);

        // pin three times
        MockPage page0 = (MockPage) am.pinPage(tid0, pid0, pm);
        am.pinPage(tid0, pid0, pm);
        am.pinPage(tid0, pid0, pm);

        assertEquals(3, bm.pinCount(pid0));

        am.transactionComplete(tid0, false); // abort

        assertEquals(0, bm.pinCount(pid0));
    }

}
