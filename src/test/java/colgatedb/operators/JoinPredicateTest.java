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
public class JoinPredicateTest {

    /**
     * Unit test for JoinPredicate.filter()
     */
    @Test
    @GradedTest(number="13.1", max_score=2.0, visibility="visible")
    public void filterVaryingVals() {
        int[] vals = new int[] { -1, 0, 1 };

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Op.EQUALS, 0);
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Op.GREATER_THAN, 0);
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i - 1, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Op.GREATER_THAN_OR_EQ, 0);
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Op.LESS_THAN, 0);
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i - 1, 1)));
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i + 1, 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Op.LESS_THAN_OR_EQ, 0);
            assertFalse(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i - 1, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i, 1)));
            assertTrue(p.filter(TestUtility.getIntTuple(i, 1), TestUtility.getIntTuple(i + 1, 1)));
        }
    }
}

