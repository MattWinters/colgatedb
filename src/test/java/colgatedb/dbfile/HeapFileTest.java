package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
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

public class HeapFileTest {

    private final String tableName = "sometable";
    private final TransactionId tid = new TransactionId();

    @Before
    public void setUp() {
        Database.reset();
    }

    @Test
    @GradedTest(number="9.1", max_score=1.0, visibility="visible")
    public void testGetId() throws IOException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File emptyFile = File.createTempFile("table", ".dat");
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);
        assertEquals(Database.getCatalog().getTableId(tableName), hf.getId());
    }

    @Test
    @GradedTest(number="9.2", max_score=1.0, visibility="visible")
    public void testTupleDesc() throws IOException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File emptyFile = File.createTempFile("table", ".dat");
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);
        assertEquals(td, hf.getTupleDesc());
    }

    @Test
    @GradedTest(number="9.3", max_score=1.0, visibility="visible")
    public void testNumPages() throws IOException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File emptyFile = File.createTempFile("table", ".dat");
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);
        assertEquals(0, hf.numPages());
    }

    @Test
    @GradedTest(number="9.4", max_score=1.0, visibility="visible")
    public void testNumPages2() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File dataFile = File.createTempFile("table", ".dat");
        int tableid = -1;

        // allocate one page to this heapfile
        DiskManagerImpl tempDM = new DiskManagerImpl(Database.getPageSize());
        tempDM.addFileEntry(tableid, dataFile.getAbsolutePath());
        tempDM.allocatePage(new SimplePageId(tableid, 0));

        HeapFile hf = Catalog.addHeapFile(tableName, td, dataFile);

        assertEquals(1, hf.numPages());
    }

    @Test
    @GradedTest(number="9.5", max_score=1.0, visibility="visible")
    public void testSimpleInsert() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File dataFile = File.createTempFile("table", ".dat");
        int tableid = -1;
        SimplePageId pid = new SimplePageId(tableid, 0);

        // allocate one page to this heapfile
        DiskManagerImpl tempDM = new DiskManagerImpl(Database.getPageSize());
        tempDM.addFileEntry(tableid, dataFile.getAbsolutePath());
        tempDM.allocatePage(pid);

        HeapFile hf = Catalog.addHeapFile(tableName, td, dataFile);

        // insert tuple in empty heapfile... should be plenty of room on this page
        Tuple insertedTuple = TestUtility.getIntTuple(new int[]{-1, 1});

        // insert: check record id before and after
        assertNull(insertedTuple.getRecordId());
        hf.insertTuple(tid, insertedTuple);
        assertNotNull(insertedTuple.getRecordId());

        assertEquals(1, hf.numPages());  // should still be one

        Database.getBufferManager().flushAllPages();  // persist changes to disk

        SlottedPage page = (SlottedPage) tempDM.readPage(pid, new SlottedPageMaker(td, Database.getPageSize()));
        Tuple foundTuple = page.iterator().next();
        assertEqualTuples(insertedTuple, foundTuple);
    }

    @Test
    @GradedTest(number="9.6", max_score=1.0, visibility="visible")
    public void testPageIsAllocatedIfNeeded() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File emptyFile = File.createTempFile("table", ".dat");
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);

        assertEquals(0, hf.numPages());

        // insert tuple in empty heapfile... should cause a page to be allocated
        Tuple insertedTuple = TestUtility.getIntTuple(new int[]{-1, 1});
        assertNull(insertedTuple.getRecordId());
        hf.insertTuple(tid, insertedTuple);

        // record id should no longer be null
        assertNotNull(insertedTuple.getRecordId());
        // a page should be allocated...  is heapfile correctly keeping track of num pages?
        assertEquals(1, hf.numPages());
    }

    @Test
    @GradedTest(number="9.7", max_score=1.0, visibility="visible")
    public void testPageIsAllocatedIfNeeded2() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File dataFile = File.createTempFile("table", ".dat");
        int tableid = -1;
        SlottedPageMaker pm = new SlottedPageMaker(td, Database.getPageSize());
        SimplePageId pid0 = new SimplePageId(tableid, 0);
        SimplePageId pid1 = new SimplePageId(tableid, 1);

        // allocate one full page to this heapfile
        DiskManagerImpl tempDM = new DiskManagerImpl(Database.getPageSize());
        tempDM.addFileEntry(tableid, dataFile.getAbsolutePath());
        tempDM.allocatePage(pid0);
        SlottedPage page0 = (SlottedPage) tempDM.readPage(pid0, pm);
        for (int slotno=0; slotno < page0.getNumSlots(); slotno++) {
            page0.insertTuple(TestUtility.getIntTuple(new int[]{0, slotno}));
        }
        tempDM.writePage(page0);


        HeapFile hf = Catalog.addHeapFile(tableName, td, dataFile);

        // insert tuple in empty heapfile... should cause a page to be allocated
        Tuple insertedTuple = TestUtility.getIntTuple(new int[]{1, 0});

        // insert: check record id before and after
        assertNull(insertedTuple.getRecordId());
        hf.insertTuple(tid, insertedTuple);
        assertNotNull(insertedTuple.getRecordId());

        assertEquals(2, hf.numPages());  // should now be two

        Database.getBufferManager().flushAllPages();  // persist changes to disk

        SlottedPage page = (SlottedPage) tempDM.readPage(pid1, pm);
        Tuple foundTuple = page.iterator().next();
        assertEqualTuples(insertedTuple, foundTuple);
    }

    @Test
    @GradedTest(number="9.8", max_score=1.0, visibility="visible")
    public void testDelete() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        File dataFile = File.createTempFile("table", ".dat");
        int tableid = -1;
        SlottedPageMaker pm = new SlottedPageMaker(td, Database.getPageSize());
        SimplePageId pid = new SimplePageId(tableid, 0);

        // allocate one page to this heapfile
        DiskManagerImpl tempDM = new DiskManagerImpl(Database.getPageSize());
        tempDM.addFileEntry(tableid, dataFile.getAbsolutePath());
        tempDM.allocatePage(pid);

        HeapFile hf = Catalog.addHeapFile(tableName, td, dataFile);

        // insert tuple in empty heapfile... should be plenty of room on this page
        Tuple tuple = TestUtility.getIntTuple(new int[]{-1, 1});
        hf.insertTuple(tid, tuple);
        assertNotNull(tuple.getRecordId());

        // insert: check record id before and after
        hf.deleteTuple(tid, tuple);
        assertNull(tuple.getRecordId());

        Database.getBufferManager().flushAllPages();  // persist changes to disk

        SlottedPage page = (SlottedPage) tempDM.readPage(pid, pm);
        assertEquals(page.getNumSlots(), page.getNumEmptySlots());
    }

    @Test
    @GradedTest(number="9.9", max_score=1.0, visibility="visible")
    public void testIterator() throws IOException, TransactionAbortedException {
        TupleDesc td = TestUtility.getTupleDesc(2);
        int pageSize = 64;
        Database.setPageSize(pageSize);
        Database.setBufferPoolSize(1);  // make sure unused pages are being unpinned!

        int numPages = 4;

        PageMaker pm = new SlottedPageMaker(td, pageSize);

        DiskManagerImpl tempDM = new DiskManagerImpl(pageSize);

        // create a table that has 4 pages
        File file = File.createTempFile("table", ".dat");
        String filename = file.getAbsolutePath();
        int tableid = -1; //filename.hashCode();
        tempDM.addFileEntry(tableid, filename);
        List<Tuple> tups = new LinkedList<Tuple>();

        for (int pageNo = 0; pageNo < numPages; pageNo++) {
            SimplePageId pid = new SimplePageId(tableid, pageNo);
            tempDM.allocatePage(pid);
            SlottedPage page = (SlottedPage) tempDM.readPage(pid, pm);
            Tuple t1 = TestUtility.getIntTuple(new int[]{pageNo, 1});
            Tuple t2 = TestUtility.getIntTuple(new int[]{pageNo, 2});
            page.insertTuple(t1);
            page.insertTuple(t2);
            tups.add(t1);
            tups.add(t2);
            tempDM.writePage(page);
        }

        String tableName = "blah";
        HeapFile hf = Catalog.addHeapFile(tableName, td, file);

        Iterator<Tuple> tupIter = tups.iterator();
        DbFileIterator iterator = hf.iterator(new TransactionId());
        iterator.open();
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


}
