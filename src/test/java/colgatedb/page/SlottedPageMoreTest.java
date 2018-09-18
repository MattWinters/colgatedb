package colgatedb.page;

import colgatedb.TestUtility;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

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
public class SlottedPageMoreTest {
    private PageId pid = new SimplePageId(0, 0);
    private static final int DEFAULT_PAGE_SIZE = 128;

    /**
     * Helper function to make a SlottedPage with SimplePageId(0,0)
     * @param pageSize
     * @param numColumns number of columns in schema (all Type.INT_TYPE)
     * @return
     */
    private SlottedPage makePage(int pageSize, int numColumns) {
        TupleDesc td = TestUtility.getTupleDesc(numColumns);
        return new SlottedPage(pid, td, pageSize);
    }

    private SlottedPage makePage(int numColumns) {
        return makePage(DEFAULT_PAGE_SIZE, numColumns);
    }


    @Test
    @GradedTest(number="6.1", max_score=1.0, visibility="after_due_date")
    public void testDelete() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        Tuple t = TestUtility.getIntTuple(0, numColumns);
        page.insertTuple(t);
        assertEquals(slots - 1, page.getNumEmptySlots());
        page.deleteTuple(t);
        assertEquals(slots, page.getNumEmptySlots());
        assertNull(t.getRecordId());
    }

    /**
     * Check that a tuple's RecordId is updated after it is inserted into page.
     */
    @Test
    @GradedTest(number="6.2", max_score=1.0, visibility="after_due_date")
    public void updatedRecordIdAfterInsertSlot() {
        int numColumns = 1;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        Tuple tuple = TestUtility.getIntTuple(numColumns);
        assertNull(tuple.getRecordId());
        page.insertTuple(0, tuple);
        assertNotNull(tuple.getRecordId());
        assertEquals(pid, tuple.getRecordId().getPageId());  // page id should match
        assertEquals(0, tuple.getRecordId().tupleno());      // make sure tuple number matches
    }


    /**
     * Try to delete a tuple without a matching page id
     */
    @Test
    @GradedTest(number="6.3", max_score=1.0, visibility="after_due_date")
    public void deleteBadTuple4() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        // tuple has record id but pageid doesn't match
        Tuple t2 = TestUtility.getIntTuple(numColumns);
        page.insertTuple(t2);
        Tuple t3 = TestUtility.getIntTuple(numColumns);
        // now muck with the RecordId giving it the correct tupleno but wrong page id
        t3.setRecordId(new RecordId(new SimplePageId(0, 1), t2.getRecordId().tupleno()));
        try {
            page.deleteTuple(t3);
            fail("RecordId doesn't match.  Should raise a PageException!");
        } catch (PageException e) {
            // expected
        }
    }

    @Test
    @GradedTest(number="6.4", max_score=1.0, visibility="after_due_date")
    public void testIteratorOnEmptyPage() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        Iterator<Tuple> iterator = page.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    @GradedTest(number="6.5", max_score=1.0, visibility="after_due_date")
    public void testNextWithoutHasNext() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        Iterator<Tuple> iterator = page.iterator();
        try {
            iterator.next();
            fail("There is no next.  Should throw a NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        }

        List<Tuple> tuples = new LinkedList<>();
        Tuple t = TestUtility.getIntTuple(2000, numColumns);
        page.insertTuple(0, t);
        tuples.add(t);
        iterator = page.iterator();
        for (Tuple expectedTuple : tuples) {
            Tuple nextTuple = iterator.next();
            assertEquals(expectedTuple, nextTuple);
        }
        try {
            iterator.next();
            fail("There is no next.  Should throw a NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    @Test
    @GradedTest(number="6.6", max_score=1.0, visibility="after_due_date")
    public void testIteratorWithOneNonEmptySlot() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();

        // insert into one slot
        List<Tuple> tuples = new LinkedList<>();
        Tuple t = TestUtility.getIntTuple(2000, numColumns);
        page.insertTuple(slots/2, t);
        tuples.add(t);

        Iterator<Tuple> iterator = page.iterator();
        for (Tuple expectedTuple : tuples) {
            assertTrue(iterator.hasNext());
            Tuple nextTuple = iterator.next();
            assertEquals(expectedTuple, nextTuple);
        }
        assertFalse(iterator.hasNext());

        try {
            iterator.next();
            fail("There is no next.  Should throw a NoSuchElementException.");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

}
