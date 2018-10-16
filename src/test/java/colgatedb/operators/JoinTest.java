package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.TestUtility;
import colgatedb.page.PageTestUtility;
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
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */
public class JoinTest {

    int width1 = 2;
    int width2 = 3;
    DbIterator scan1;
    DbIterator scan2;
    DbIterator eqJoin;
    DbIterator gtJoin;

    /**
     * Initialize each unit test
     */
    @Before
    public void createTupleLists() throws Exception {
        this.scan1 = OperatorTestUtility.createTupleList(width1,
                new int[]{1, 2,
                        3, 4,
                        5, 6,
                        7, 8});
        this.scan2 = OperatorTestUtility.createTupleList(width2,
                new int[]{1, 2, 3,
                        2, 3, 4,
                        3, 4, 5,
                        4, 5, 6,
                        5, 6, 7});
        this.eqJoin = OperatorTestUtility.createTupleList(width1 + width2,
                new int[]{1, 2, 1, 2, 3,
                        3, 4, 3, 4, 5,
                        5, 6, 5, 6, 7});
        this.gtJoin = OperatorTestUtility.createTupleList(width1 + width2,
                new int[]{
                        3, 4, 1, 2, 3, // 1, 2 < 3
                        3, 4, 2, 3, 4,
                        5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
                        5, 6, 2, 3, 4,
                        5, 6, 3, 4, 5,
                        5, 6, 4, 5, 6,
                        7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
                        7, 8, 2, 3, 4,
                        7, 8, 3, 4, 5,
                        7, 8, 4, 5, 6,
                        7, 8, 5, 6, 7});
    }

    @Test
    @GradedTest(number="14.1", max_score=1.0, visibility="visible")
    public void getJoinPredicate() {
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        assertEquals(pred, op.getJoinPredicate());
    }

    @Test
    @GradedTest(number="14.2", max_score=1.0, visibility="visible")
    public void getTupleDesc() {
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        TupleDesc expected = TestUtility.getTupleDesc(width1 + width2);
        TupleDesc actual = op.getTupleDesc();
        assertEquals(expected, actual);
    }

    @Test
    @GradedTest(number="14.3", max_score=1.0, visibility="visible")
    public void setChildrenIncorrectly() {
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        DbIterator[] children = {scan1};
        try {
            op.setChildren(children);
            fail("should have raised an exception!");
        } catch (DbException e) {
            // expected
        }
    }

    /**
     * Unit test that uses an = predicate.  Joins scan1 and scan2 based on
     * the first attribute in each relation.  Each tuple in scan1 has at most
     * one match in scan2 (and vice versa).  Some tuples have zero matches.
     */
    @Test
    @GradedTest(number="14.4", max_score=1.0, visibility="visible")
    public void eqJoin() throws Exception {
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        eqJoin.open();
        OperatorTestUtility.matchAllTuples(eqJoin, op);
    }

    @Test
    @GradedTest(number="14.5", max_score=1.0, visibility="visible")
    public void rewind() throws Exception {
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        while (op.hasNext()) {
            assertNotNull(op.next());
        }
        assertTrue(OperatorTestUtility.checkExhausted(op));
        op.rewind();

        eqJoin.open();
        Tuple expected = eqJoin.next();
        Tuple actual = op.next();
        assertTrue(PageTestUtility.compareTuples(expected, actual));
    }

    @Test
    @GradedTest(number="14.6", max_score=1.0, visibility="visible")
    public void gtJoin() throws Exception {
        JoinPredicate pred = new JoinPredicate(0, Op.GREATER_THAN, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        gtJoin.open();
        OperatorTestUtility.matchAllTuples(gtJoin, op);
    }

    @Test
    @GradedTest(number="14.7", max_score=1.0, visibility="visible")
    public void simpleJoinAtMostOneMatch() throws Exception {
        int width1 = 1;
        int width2 = 2;
        TupleIterator scan1 = OperatorTestUtility.createTupleList(width1,
                new int[]{1,
                        3,
                        5,
                        7});
        TupleIterator scan2 = OperatorTestUtility.createTupleList(width2,
                new int[]{1,10,
                        5,20});
        TupleIterator result = OperatorTestUtility.createTupleList(width1+width2,
                new int[]{1,1,10,
                        5,5,20});
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        result.open();
        OperatorTestUtility.matchAllTuples(result, op);
    }

    @Test
    @GradedTest(number="14.8", max_score=1.0, visibility="visible")
    public void moreThanOneMatchOnRight() throws Exception {
        int width1 = 1;
        int width2 = 2;
        TupleIterator scan1 = OperatorTestUtility.createTupleList(width1,
                new int[]{1,
                        3,
                        5,
                        7});
        TupleIterator scan2 = OperatorTestUtility.createTupleList(width2,
                new int[]{1,10,
                        5,20,
                        1,30,
                        6,-1,
                        5,40,
                        3,50,
                        3,60,
                        3,70,});
        TupleIterator result = OperatorTestUtility.createTupleList(width1+width2,
                new int[]{1,1,10,
                        5,5,20,
                        1,1,30,
                        5,5,40,
                        3,3,50,
                        3,3,60,
                        3,3,70,
                });
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        result.open();
        OperatorTestUtility.matchAllTuples(result, op);
    }

    @Test
    @GradedTest(number="14.9", max_score=1.0, visibility="visible")
    public void moreThanOneMatchOnLeft() throws Exception {
        int width1 = 1;
        int width2 = 2;
        TupleIterator scan1 = OperatorTestUtility.createTupleList(width2,
                new int[]{1,10,
                        5,20,
                        1,30,
                        5,40,
                        7,50});
        TupleIterator scan2 = OperatorTestUtility.createTupleList(width1,
                new int[]{1,
                        5,
                        6,
                        7,
                });
        TupleIterator result = OperatorTestUtility.createTupleList(width1+width2,
                new int[]{1,10,1,
                        5,20,5,
                        1,30,1,
                        5,40,5,
                        7,50,7,
                });
        JoinPredicate pred = new JoinPredicate(0, Op.EQUALS, 0);
        Join op = new Join(pred, scan1, scan2);
        op.open();
        result.open();
        OperatorTestUtility.matchAllTuples(result, op);
    }
}

