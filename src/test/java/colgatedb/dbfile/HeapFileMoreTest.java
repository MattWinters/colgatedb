package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static colgatedb.page.PageTestUtility.assertEqualTuples;
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

public class HeapFileMoreTest {

    private final String tableName = "sometable";
    private final TransactionId tid = new TransactionId();
    static final int pageSize = 64;
    static final TupleDesc td = TestUtility.getTupleDesc(2);
    static final PageMaker pm = new SlottedPageMaker(td, pageSize);

    @Before
    public void setUp() {
        Database.reset();
    }

    @Test
    @GradedTest(number="10.1", max_score=1.0, visibility="visible")
    public void notOpen() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();

        HeapFile hf = initializeHeapFile(tups);

        DbFileIterator iterator = hf.iterator(new TransactionId());
        assertFalse(iterator.hasNext());  // iterator is not open
    }

    @Test
    @GradedTest(number="10.2", max_score=1.0, visibility="visible")
    public void testOpenClose() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();

        HeapFile hf = initializeHeapFile(tups);

        DbFileIterator iterator = hf.iterator(new TransactionId());
        iterator.open();
        iterator.close();
        assertFalse(iterator.hasNext());  // iterator is not open
    }

    @Test
    @GradedTest(number="10.3", max_score=1.0, visibility="visible")
    public void testRewind() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();

        HeapFile hf = initializeHeapFile(tups);
        DbFileIterator iterator = hf.iterator(new TransactionId());
        iterator.open();
        iterator.next();
        iterator.next();
        iterator.next();
        iterator.rewind();  // back to beginning

        assertIteratorsMatch(tups, iterator);
    }

    @Test
    @GradedTest(number="10.4", max_score=1.0, visibility="visible")
    public void testBadTuple() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();
        HeapFile hf = initializeHeapFile(new int[]{3}, tups);
        Tuple t = tups.get(0);
        t.setRecordId(new RecordId(new SimplePageId(hf.getId(), 0), 3));   // only 3 tuples, no tuple in slot 3
        try {
            hf.deleteTuple(tid, t);
            fail("Should raise an exception");
        } catch (PageException | DbException e) {
            // expected
        }
    }

    @Test
    @GradedTest(number="10.5", max_score=1.0, visibility="visible")
    public void testInsertSkipsFullPages() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();
        HeapFile hf = initializeHeapFile(new int[]{-1,-1,0}, tups); // first two pages full
        Tuple t = TestUtility.getIntTuple(new int[]{10, 10});
        hf.insertTuple(tid, t);
        assertNotNull(t.getRecordId());
        assertEquals(2, t.getRecordId().getPageId().pageNumber());
    }

    @Test
    @GradedTest(number="10.6", max_score=1.0, visibility="visible")
    public void testDeleteOnMultiplePages() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();
        HeapFile hf = initializeHeapFile(new int[]{3,3,3}, tups);
        tups.clear();

        DbFileIterator hfIterator = hf.iterator(tid);
        hfIterator.open();
        while (hfIterator.hasNext()) {
            Tuple t = hfIterator.next();
            tups.add(t);
        }

        Database.getBufferManager().evictDirty(true);
        for (Tuple t: tups) {
            hf.deleteTuple(tid, t);
        }

        hfIterator = hf.iterator(tid);
        hfIterator.open();
        assertFalse(hfIterator.hasNext());
        hfIterator.close();
    }

    @Test
    @GradedTest(number="10.7", max_score=1.0, visibility="visible")
    public void testEmptyPagesAtStart() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();
        HeapFile hf = initializeHeapFile(new int[]{0,0,0,0,0,1}, tups);
        DbFileIterator iterator = hf.iterator(new TransactionId());
        iterator.open();
        assertIteratorsMatch(tups, iterator);
    }

    @Test
    @GradedTest(number="10.8", max_score=1.0, visibility="visible")
    public void testEmptyPagesAtEnd() throws IOException, TransactionAbortedException {
        List<Tuple> tups = new LinkedList<Tuple>();
        HeapFile hf = initializeHeapFile(new int[]{1,0,0,0,0,0}, tups);
        DbFileIterator iterator = hf.iterator(new TransactionId());
        iterator.open();
        assertIteratorsMatch(tups, iterator);
    }

    /**
     * Given a list of expectedTuples and an *open* DBFileIterator, check that
     * the DBFileIterator's output matches the contents of expectedTuples.
     * @param expectedTuples
     * @param iterator
     * @throws TransactionAbortedException
     */
     private static void assertIteratorsMatch(List<Tuple> expectedTuples, DbFileIterator iterator) throws TransactionAbortedException {
        Iterator<Tuple> tupIter = expectedTuples.iterator();
        int tupleNo = 0;
        while (tupIter.hasNext()) {
            System.out.println("tupleNo = " + tupleNo++);
            Tuple nextFromList = tupIter.next();
            assertTrue(iterator.hasNext());
            Tuple nextFromHF = iterator.next();
            assertEqualTuples(nextFromList, nextFromHF);
        }
        assertFalse(iterator.hasNext());
    }

    static HeapFile initializeHeapFile(List<Tuple> tups) throws IOException {
        return initializeHeapFile(new int[]{2,2,2,2}, tups);
    }

    /**
     * Populates the heapfile with data.
     * @param tupsPerPage an array indicating number of pages and how many tuples should be on each page
     * @param tups inserted records are added this list
     * @return a heapfile
     * @throws IOException
     */
    static HeapFile initializeHeapFile(int[] tupsPerPage, List<Tuple> tups) throws IOException {
        Database.setPageSize(pageSize);
        Database.setBufferPoolSize(1);  // make sure unused pages are being unpinned!

        DiskManagerImpl tempDM = new DiskManagerImpl(pageSize);

        // create a table that has 4 pages
        File file = File.createTempFile("table", ".dat");
        String filename = file.getAbsolutePath();
        int tableid = -1; //filename.hashCode();
        tempDM.addFileEntry(tableid, filename);

        for (int pageNo = 0; pageNo < tupsPerPage.length; pageNo++) {
            SimplePageId pid = new SimplePageId(tableid, pageNo);
            tempDM.allocatePage(pid);
            SlottedPage page = (SlottedPage) tempDM.readPage(pid, pm);
            int tupsOnThisPage = tupsPerPage[pageNo];
            if (tupsOnThisPage == -1) {
                tupsOnThisPage = page.getNumEmptySlots();
            }
            for (int i = 0; i < tupsOnThisPage; i++) {
                Tuple t = TestUtility.getIntTuple(new int[]{pageNo, i});
                page.insertTuple(t);
                tups.add(t);
            }
            tempDM.writePage(page);
        }

        String tableName = "blah";
        return Catalog.addHeapFile(tableName, td, file);
    }


}
