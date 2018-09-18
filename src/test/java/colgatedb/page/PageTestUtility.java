package colgatedb.page;

import colgatedb.tuple.Tuple;

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
public class PageTestUtility {

    /**
     * @return true iff the tuples have the same number of fields and
     *   corresponding fields in the two Tuples are all equal.
     */
    public static boolean compareTuples(Tuple t1, Tuple t2) {
        if (t1.getTupleDesc().numFields() != t2.getTupleDesc().numFields())
            return false;

        for (int i = 0; i < t1.getTupleDesc().numFields(); ++i) {
            if (!(t1.getTupleDesc().getFieldType(i).equals(t2.getTupleDesc().getFieldType(i))))
                return false;
            if (!(t1.getField(i).equals(t2.getField(i))))
                return false;
        }

        return true;
    }

    /**
     * @throws AssertionError if tuples are "unequal"
     */
    public static void assertEqualTuples(Tuple t1, Tuple t2) {
        assertEquals(t1.getTupleDesc().numFields(), t2.getTupleDesc().numFields());

        for (int i = 0; i < t1.getTupleDesc().numFields(); ++i) {
            assertEquals("Field types for field " + i + " don't match",
                    t1.getTupleDesc().getFieldType(i), t2.getTupleDesc().getFieldType(i));
            assertEquals("Field values for field " + i + " don't match",
                    t1.getField(i), t2.getField(i));
        }
    }

    /**
     * @throws AssertionError if pages are "unequal"
     */
    public static void assertEqualPages(SlottedPage expected, SlottedPage actual) {
        int numSlots = expected.getNumSlots();
        assertNotNull(actual);
        assertEquals(numSlots, actual.getNumSlots());
        assertEquals(expected.getNumEmptySlots(), actual.getNumEmptySlots());

        for (int i = 0; i < numSlots; i++) {
            if (expected.isSlotEmpty(i)) {
                assertTrue("Slot " + i + " should be empty.", actual.isSlotEmpty(i));
            } else {
                assertTrue("Slot " + i + " should not be empty.", actual.isSlotUsed(i));
                Tuple expectedTuple = expected.getTuple(i);
                Tuple actualTuple = actual.getTuple(i);
                try {
                    assertEqualTuples(expectedTuple, actualTuple);
                } catch (AssertionError e) {
                    throw new AssertionError("Tuples at slot " + i + " do not match", e);
                }
            }
        }
    }

    public static void assertUnEqualPages(SlottedPage expected, SlottedPage actual) {
        try {
            assertEqualPages(expected, actual);
            fail("Pages are equal but should not be!");
        } catch (AssertionError e) {
            // expected
        }
    }

}
