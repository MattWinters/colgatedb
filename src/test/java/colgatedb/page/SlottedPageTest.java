package colgatedb.page;

import colgatedb.TestUtility;
import colgatedb.page.*;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;
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
public class SlottedPageTest {
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
    public void getId() {
        SlottedPage page = makePage(32, 4);
        assertEquals(pid, page.getId());
    }

    @Test
    public void testIsSlotUsed() {
        SlottedPage page = makePage(32, 4);
        int slots = page.getNumEmptySlots();
        for (int i = 0; i < slots; i++) {
            assertFalse(page.isSlotUsed(i));
            assertTrue(page.isSlotEmpty(i));
        }
    }

    /**
     * Try to insert a tuple with 5 columns of ints into a page that expects
     * 5 columns of ints.
     */
    @Test
    public void insertBadTuple() {
        int numColumns = 4;
        SlottedPage page = makePage(numColumns);
        try {
            page.insertTuple(TestUtility.getIntTuple(numColumns + 1));
            fail("Should not be able to insert bad tuple.");
        } catch (PageException e) {
            // expected
        }
    }

    /**
     * Insert into specific slots, check error if insert into occupied slot.
     */
    @Test
    public void insertTupleIntoSlot() {
        int numColumns = 4;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        for (int i = 0; i < slots-1; i++) {
            Tuple t = TestUtility.getIntTuple(numColumns);
            page.insertTuple(i, t);
            assertEquals(slots - (i+1), page.getNumEmptySlots());
            Tuple t2 = page.getTuple(i);
            assertEquals(t, t2);
        }
        try {
            page.insertTuple(0, TestUtility.getIntTuple(numColumns));
            fail("Should not be able to insert tuple into an occupied slot.");
        } catch (PageException e) {
            // expected
        }
    }

    /**
     * Try to insert a tuple into page that is full
     */
    @Test
    public void insertTupleIntoFullPage() {
        int numColumns = 4;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        for (int i = 0; i < slots; i++) {
            page.insertTuple(TestUtility.getIntTuple(numColumns));
            assertEquals(slots - (i+1), page.getNumEmptySlots());
        }
        // now page should be full...

        try {
            page.insertTuple(TestUtility.getIntTuple(numColumns));
            fail("Should not be able to insert tuple into full page.");
        } catch (PageException e) {
            // expected
        }
    }

    /**
     * Check that a tuple's RecordId is updated after it is inserted into page.
     */
    @Test
    public void updatedRecordIdAfterInsert() {
        int numColumns = 1;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        Tuple tuple = TestUtility.getIntTuple(numColumns);
        assertNull(tuple.getRecordId());
        page.insertTuple(tuple);
        assertNotNull(tuple.getRecordId());
        assertEquals(pid, tuple.getRecordId().getPageId());  // page id should match
        int tupleno = tuple.getRecordId().tupleno();
        assertTrue(0 <= tupleno && tupleno < slots);         // make sure tuple number is reasonable
    }



    /**
     * Try to delete a tuple without a record id.
     */
    @Test
    public void deleteBadTuple() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);

        // tuple has no record id
        Tuple t1 = TestUtility.getIntTuple(numColumns);
        try {
            page.deleteTuple(t1);
            fail("RecordId is null.  Should raise a PageException!");
        } catch (NullPointerException e) {
            fail("Should not throw this kind of exception.");
        } catch (PageException e) {
            // expected
        }
    }

    /**
     * Try to delete a tuple without a matching page id
     */
    @Test
    public void deleteBadTuple2() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        // tuple has record id but pageid doesn't match
        Tuple t2 = TestUtility.getIntTuple(numColumns);
        page.insertTuple(t2);
        // now muck with the RecordId giving it the correct tupleno but wrong page id
        t2.setRecordId(new RecordId(new SimplePageId(0, 1), t2.getRecordId().tupleno()));
        try {
            page.deleteTuple(t2);
            fail("RecordId doesn't match.  Should raise a PageException!");
        } catch (PageException e) {
            // expected
        }
    }


    /**
     * Try to delete a tuple from a slot that's empty
     */
    @Test
    public void deleteBadTuple3() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);

        // tuple has record id but the slot is empty
        Tuple t3 = TestUtility.getIntTuple(numColumns);
        page.insertTuple(t3);
        RecordId rid = t3.getRecordId();
        // now delete the tuple
        page.deleteTuple(t3);
        t3.setRecordId(rid);  // reset rid in case implementation nullifies the record id
        try {
            page.deleteTuple(t3);
            fail("Slot is empty.  Should raise a PageException!");
        } catch (PageException e) {
            // expected
        }
    }


    /**
     * Fill up page, delete one tuple, then do one more insert.
     */
    @Test
    public void insertAfterDelete() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        int slots = page.getNumEmptySlots();
        Tuple[] tuples = new Tuple[slots];
        for (int i = 0; i < slots; i++) {
            Tuple t = TestUtility.getIntTuple(i, numColumns);
            page.insertTuple(t);
            tuples[i] = t;
            assertEquals(slots - (i+1), page.getNumEmptySlots());
        }
        // now page should be full...
        assertEquals(0, page.getNumEmptySlots());

        // delete one of the tuples and insert a fresh one
        page.deleteTuple(tuples[0]);
        assertEquals(1, page.getNumEmptySlots());
        page.insertTuple(TestUtility.getIntTuple(-1, numColumns));
        assertEquals(0, page.getNumEmptySlots());
    }

    @Test
    public void testIteratorRemove() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);
        Iterator<Tuple> iterator = page.iterator();

        try {
            iterator.remove();
            fail("Remove should not be supported.  Throw an UnsupportedOperationException.");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testIteratorOnFullPage() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);  // just enough room for 3 tuples
        int slots = page.getNumEmptySlots();

        // insert into every slot
        List<Tuple> tuples = new LinkedList<>();
        for (int i = 0; i < slots; i++) {
            Tuple t = TestUtility.getIntTuple(i, numColumns);
            page.insertTuple(i, t);
            tuples.add(t);
        }
        assertEquals(0, page.getNumEmptySlots());

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

    @Test
    public void testIteratorWithEmptySlots() {
        int numColumns = 2;
        SlottedPage page = makePage(numColumns);  // just enough room for 3 tuples
        int slots = page.getNumEmptySlots();

        // insert into every other slot
        List<Tuple> tuples = new LinkedList<>();

        for (int i = 0; i < slots; i+=2) {
            Tuple t = TestUtility.getIntTuple(i, numColumns);
            page.insertTuple(i, t);
            tuples.add(t);
        }

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
