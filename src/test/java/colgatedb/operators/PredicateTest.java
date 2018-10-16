package colgatedb.operators;

import colgatedb.TestUtility;
import colgatedb.tuple.Op;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
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
public class PredicateTest {

    @Test
    @GradedTest(number="11.1", max_score=2.0, visibility="visible")
    public void filter() {
        int[] vals = new int[] { -1, 0, 1 };

        for (int i : vals) {
            Predicate p = new Predicate(0, Op.EQUALS, OperatorTestUtility.getField(i));
            assertFalse(p.filter(TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Op.GREATER_THAN,
                    OperatorTestUtility.getField(i));
            assertFalse(p.filter(TestUtility.getIntTuple(i - 1, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Op.GREATER_THAN_OR_EQ,
                    OperatorTestUtility.getField(i));
            assertFalse(p.filter(TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Op.LESS_THAN,
                    OperatorTestUtility.getField(i));
            assertTrue(p.filter(TestUtility.getIntTuple(i - 1, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Op.LESS_THAN_OR_EQ,
                    OperatorTestUtility.getField(i));
            assertTrue(p.filter(TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i + 1, 1)));
        }
    }

}

