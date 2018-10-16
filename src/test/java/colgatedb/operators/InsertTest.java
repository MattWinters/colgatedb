package colgatedb.operators;

import colgatedb.Database;
import colgatedb.TestUtility;
import colgatedb.dbfile.HeapFile;
import colgatedb.dbfile.TestUtilHeapFile;
import colgatedb.page.PageTestUtility;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.TupleDesc;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

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

public class InsertTest {

    private DbIterator scan1;
    private TransactionId tid;
    private HeapFile empty;

    @Before
    public void setUp() throws Exception {
        Database.reset();
        empty = TestUtilHeapFile.createHeapFile(2);
        this.scan1 = OperatorTestUtility.createTupleList(2,
                new int[]{1, 2,
                        1, 4,
                        1, 6,
                        3, 2,
                        3, 4,
                        3, 6,
                        5, 7});
        tid = new TransactionId();
    }

    @Test
    @GradedTest(number="15.1", max_score=1.0, visibility="visible")
    public void getTupleDesc() throws Exception {
        Insert op = new Insert(tid, scan1, empty.getId());
        TupleDesc expected = TestUtility.getTupleDesc(1);
        TupleDesc actual = op.getTupleDesc();
        assertEquals(expected, actual);
        assertEquals("count", actual.getFieldName(0));
    }

    @Test
    @GradedTest(number="15.2", max_score=1.0, visibility="visible")
    public void getNext() throws Exception {
        Insert op = new Insert(tid, scan1, empty.getId());
        op.open();
        assertTrue(PageTestUtility.compareTuples(
                TestUtility.getIntTuple(7, 1), // the length of scan1
                op.next()));
        assertEquals(1, empty.numPages());
    }
}

