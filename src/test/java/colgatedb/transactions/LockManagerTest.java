package colgatedb.transactions;

import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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

/**
 * Tests basic functionality of the LockManager w/o any concurrency.
 */
public class LockManagerTest {
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private LockManager lm;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="17.1", max_score=1.0, visibility="visible")
    public void holdsLock() throws TransactionAbortedException {
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_WRITE));
        lm.acquireLock(tid1, pid1, Permissions.READ_ONLY);
        assertTrue(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_WRITE));

        assertFalse(lm.holdsLock(tid1, pid2, Permissions.READ_ONLY));  // make sure lock is specific to page
    }

    @Test
    @GradedTest(number="17.2", max_score=1.0, visibility="visible")
    public void holdsLockExclusive() throws TransactionAbortedException {
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_WRITE));
        lm.acquireLock(tid1, pid1, Permissions.READ_WRITE);
        assertTrue(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));  // should return true b/c held lock is at least as strong
        assertTrue(lm.holdsLock(tid1, pid1, Permissions.READ_WRITE));
    }

    @Test
    @GradedTest(number="17.3", max_score=1.0, visibility="visible")
    public void releaseLock() throws TransactionAbortedException {
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
        lm.acquireLock(tid1, pid1, Permissions.READ_ONLY);
        assertTrue(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
        lm.releaseLock(tid1, pid1);
        assertFalse(lm.holdsLock(tid1, pid1, Permissions.READ_ONLY));
    }

    @Test
    @GradedTest(number="17.4", max_score=1.0, visibility="visible")
    public void releaseLockNotHeld() {
        try {
            lm.releaseLock(tid1, pid1);
            fail("Should raise exception because this txn does not hold lock");
        } catch (LockManagerException e) {
            // expected
        }
    }

    @Test
    @GradedTest(number="17.5", max_score=1.0, visibility="visible")
    public void getLockedPages() throws TransactionAbortedException {
        Set<PageId> expectedPages = new HashSet<>();
        expectedPages.add(pid1);
        expectedPages.add(pid2);

        lm.acquireLock(tid1, pid1, Permissions.READ_ONLY);
        lm.acquireLock(tid1, pid2, Permissions.READ_ONLY);

        // use sets for testing because order doesn't matter
        Set<PageId> pages = new HashSet<>(lm.getPagesForTid(tid1));
        assertEquals(expectedPages, pages);
    }

    @Test
    @GradedTest(number="17.6", max_score=1.0, visibility="visible")
    public void getTidsForPage() throws TransactionAbortedException {

        lm.acquireLock(tid1, pid1, Permissions.READ_ONLY);
        lm.acquireLock(tid2, pid2, Permissions.READ_ONLY);
        lm.acquireLock(tid3, pid2, Permissions.READ_ONLY);

        Set<TransactionId> expectedTids = new HashSet<>();
        expectedTids.add(tid1);
        assertEquals(expectedTids, new HashSet<>(lm.getTidsForPage(pid1)));

        expectedTids.clear();
        expectedTids.add(tid2);
        expectedTids.add(tid3);
        assertEquals(expectedTids, new HashSet<>(lm.getTidsForPage(pid2)));
    }
}
