package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.TestUtility;
import colgatedb.page.PageTestUtility;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Op;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

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
public class FilterTest {

    private int testWidth = 3;
    private DbIterator scan;

    @Before
    public void setUp() {
        this.scan = new OperatorTestUtility.MockScan(-5, 5, testWidth);
    }

    @Test
    @GradedTest(number="12.1", max_score=1.0, visibility="visible")
    public void getTupleDesc() {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        TupleDesc expected = TestUtility.getTupleDesc(testWidth);
        TupleDesc actual = op.getTupleDesc();
        assertEquals(expected, actual);
    }

    @Test
    @GradedTest(number="12.2", max_score=1.0, visibility="visible")
    public void getPredicate() {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        assertEquals(pred, op.getPredicate());
    }

    @Test
    @GradedTest(number="12.3", max_score=1.0, visibility="visible")
    public void getChildren() {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        assertArrayEquals(new DbIterator[]{scan}, op.getChildren());
    }

    @Test
    @GradedTest(number="12.4", max_score=1.0, visibility="visible")
    public void setChildren() {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        DbIterator[] children = {new OperatorTestUtility.MockScan(-5, 5, testWidth)};
        op.setChildren(children);
        assertArrayEquals(children, op.getChildren());
    }

    @Test
    @GradedTest(number="12.5", max_score=1.0, visibility="visible")
    public void setChildrenIncorrectly() {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        DbIterator[] children = {
                new OperatorTestUtility.MockScan(-5, 5, testWidth),
                new OperatorTestUtility.MockScan(-5, 5, testWidth)
        };

        try {
            op.setChildren(children);
            fail("should have raised an exception!");
        } catch (DbException e) {
            // expected
        }
    }

    @Test
    @GradedTest(number="12.6", max_score=1.0, visibility="visible")
    public void notYetOpen() throws TransactionAbortedException {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        assertFalse(op.hasNext());
    }

    @Test
    @GradedTest(number="12.7", max_score=1.0, visibility="visible")
    public void rewind() throws Exception {
        Predicate pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(op.hasNext());
        assertNotNull(op.next());
        assertTrue(OperatorTestUtility.checkExhausted(op));

        op.rewind();
        Tuple expected = TestUtility.getIntTuple(0, testWidth);
        Tuple actual = op.next();
        assertTrue(PageTestUtility.compareTuples(expected, actual));
        op.close();
    }

    /**
     * Unit test for Filter.getNext() using a &lt; predicate that filters
     * some tuples
     */
    @Test
    @GradedTest(number="12.8", max_score=1.0, visibility="visible")
    public void filterSomeLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Op.LESS_THAN, OperatorTestUtility.getField(2));
        Filter op = new Filter(pred, scan);
        OperatorTestUtility.MockScan expectedOut = new OperatorTestUtility.MockScan(-5, 2, testWidth);
        op.open();
        OperatorTestUtility.compareDbIterators(op, expectedOut);
        op.close();
    }

    /**
     * Unit test for Filter.getNext() using a &lt; predicate that filters
     * everything
     */
    @Test
    @GradedTest(number="12.9", max_score=1.0, visibility="visible")
    public void filterAllLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Op.LESS_THAN, OperatorTestUtility.getField(-5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(OperatorTestUtility.checkExhausted(op));
        op.close();
    }

    /**
     * Unit test for Filter.getNext() using an = predicate
     */
    @Test
    @GradedTest(number="12.10", max_score=1.0, visibility="visible")
    public void filterEqual() throws Exception {
        Predicate pred;
        this.scan = new OperatorTestUtility.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(-5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(PageTestUtility.compareTuples(TestUtility.getIntTuple(-5, testWidth),
                op.next()));
        op.close();

        this.scan = new OperatorTestUtility.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(0));
        op = new Filter(pred, scan);
        op.open();
        assertTrue(PageTestUtility.compareTuples(TestUtility.getIntTuple(0, testWidth),
                op.next()));
        op.close();

        this.scan = new OperatorTestUtility.MockScan(-5, 5, testWidth);
        pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(4));
        op = new Filter(pred, scan);
        op.open();
        assertTrue(PageTestUtility.compareTuples(TestUtility.getIntTuple(4, testWidth),
                op.next()));
        op.close();
    }

    /**
     * Unit test for Filter.getNext() using an = predicate passing no tuples
     */
    @Test
    @GradedTest(number="12.11", max_score=1.0, visibility="visible")
    public void filterEqualNoTuples() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(OperatorTestUtility.checkExhausted(op));
        op.close();
    }

}

