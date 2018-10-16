package colgatedb.operators;

import colgatedb.Database;
import colgatedb.TestUtility;
import colgatedb.dbfile.HeapFile;
import colgatedb.page.PageTestUtility;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.TupleDesc;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import static colgatedb.dbfile.TestUtilHeapFile.createHeapFile;
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
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */
public class DeleteTest {

    private TransactionId tid;
    private HeapFile hf;
    private DbIterator hfScan;

    @Before
    public void setUp() throws Exception {
        Database.reset();
        tid = new TransactionId();
        int[] data = new int[]{
                1, 2, 3,
                4, 5, 6,
                7, 8, 9
        };
        hf = createHeapFile(3, data);
        hfScan = new SeqScan(tid, hf.getId(), "");
    }

    @Test
    @GradedTest(number="16.1", max_score=1.0, visibility="visible")
    public void getTupleDesc() throws Exception {
        Delete op = new Delete(tid, hfScan);
        TupleDesc expected = TestUtility.getTupleDesc(1);
        TupleDesc actual = op.getTupleDesc();
        assertEquals(expected, actual);
        assertEquals("count", actual.getFieldName(0));
    }

    @Test
    @GradedTest(number="16.2", max_score=1.0, visibility="visible")
    public void getNext() throws Exception {
        Delete op = new Delete(tid, hfScan);
        op.open();
        assertTrue(PageTestUtility.compareTuples(
                TestUtility.getIntTuple(3, 1), // the length of hfScan
                op.next()));
    }
}

