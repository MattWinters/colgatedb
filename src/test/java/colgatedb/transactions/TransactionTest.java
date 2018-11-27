package colgatedb.transactions;

import colgatedb.AccessManager;
import colgatedb.Database;
import colgatedb.TestUtility;
import colgatedb.dbfile.HeapFile;
import colgatedb.dbfile.TestUtilHeapFile;
import colgatedb.page.*;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */
public class TransactionTest {
    private PageId p0, p1, p2;
    private TransactionId tid1, tid2;
    private AccessManager am;
    private int numCols = 2;
    private int[] magicTuple = new int[]{6, 830};

    /**
     * Set up initial resources for each unit test.
     */
    @Before
    public void setUp() throws Exception {
        Database.reset();
        HeapFile heapFile = TestUtilHeapFile.createFullHeapFile(numCols, 3);
        assertEquals(3, heapFile.numPages());

        this.p0 = new SimplePageId(heapFile.getId(), 0);
        this.p1 = new SimplePageId(heapFile.getId(), 1);
        this.p2 = new SimplePageId(heapFile.getId(), 2);
        this.tid1 = new TransactionId();
        this.tid2 = new TransactionId();

        am = Database.getAccessManager();
        am.setForce(true);
    }

    /**
     * Unit test for transactionComplete().
     * Try to acquire locks that would conflict if old locks aren't released
     * during transactionComplete().
     */
    @Test
    @GradedTest(number="22.1", max_score=1.0, visibility="visible")
    public void attemptSecondTransaction() throws Exception {
        am.acquireLock(tid1, p0, Permissions.READ_ONLY);
        am.acquireLock(tid1, p1, Permissions.READ_WRITE);
        am.transactionComplete(tid1, true);

        am.acquireLock(tid2, p0, Permissions.READ_WRITE);
        am.acquireLock(tid2, p0, Permissions.READ_WRITE);
        assertTrue(true);
    }

    /**
     * Common unit test code for transactionComplete() covering
     * commit and abort. Verify that commit persists changes to disk, and
     * that abort reverts pages to their previous on-disk state.
     */
    public void testTransactionComplete(boolean commit) throws Exception {
        am.acquireLock(tid1, p2, Permissions.READ_WRITE);
        PageMaker pm = new SlottedPageMaker(TestUtility.getTupleDesc(numCols), Database.getPageSize());
        SlottedPage p = (SlottedPage) am.pinPage(tid1, p2, pm);

        Tuple t = TestUtility.getIntTuple(magicTuple);

        p.insertTuple(t);
        am.unpinPage(tid1, p, true);
        am.transactionComplete(tid1, commit);

        am.acquireLock(tid2, p2, Permissions.READ_ONLY);
        p = (SlottedPage) am.pinPage(tid2, p2, pm);

        Iterator<Tuple> it = p.iterator();
        boolean found = false;
        while (it.hasNext()) {
            Tuple tup = it.next();
            IntField f0 = (IntField) tup.getField(0);
            IntField f1 = (IntField) tup.getField(1);
            if (f0.getValue() == magicTuple[0] && f1.getValue() == magicTuple[1]) {
                found = true;
                break;
            }
        }
        assertEquals(commit, found);
    }

    /**
     * Unit test for BufferPool.transactionComplete() assuming commit.
     * Verify that a tuple inserted during a committed transaction is durable
     */
    @Test
    @GradedTest(number="22.2", max_score=1.0, visibility="visible")
    public void commitTransaction() throws Exception {
        testTransactionComplete(true);
    }

    /**
     * Unit test for BufferPool.transactionComplete() assuming abort.
     * Verify that a tuple inserted during a committed transaction is durable
     */
    @Test
    @GradedTest(number="22.3", max_score=1.0, visibility="visible")
    public void abortTransaction() throws Exception {
        testTransactionComplete(false);
    }
}

