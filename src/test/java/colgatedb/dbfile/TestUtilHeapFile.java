package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.dbfile.HeapFile;
import colgatedb.operators.TupleIterator;
import colgatedb.page.PageMaker;
import colgatedb.page.SimplePageId;
import colgatedb.page.SlottedPage;
import colgatedb.page.SlottedPageMaker;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
public class TestUtilHeapFile {

    /**
     * Creates heap file with numPages, all empty.
     * @param numCols
     * @param numPages
     * @return
     */
    public static HeapFile createHeapFile(String tableName, int numCols, int numPages) {
        TupleDesc td = TestUtility.getTupleDesc(numCols);
        int pageSize = Database.getPageSize();
        File emptyFile;
        try {
            emptyFile = File.createTempFile(tableName, ".dat");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        emptyFile.deleteOnExit();

        int phonyTableId = -1; // temporary tableid...  the real one created when table added to catalog
        DiskManagerImpl tempDM = new DiskManagerImpl(pageSize);
        tempDM.addFileEntry(phonyTableId, emptyFile.getAbsolutePath());
        PageMaker pm = new SlottedPageMaker(td, pageSize);

        for (int pageNo = 0; pageNo < numPages; pageNo++) {
            SimplePageId pid = new SimplePageId(phonyTableId, pageNo);
            tempDM.allocatePage(pid);
            tempDM.writePage(pm.makePage(pid));
        }

        assertEquals(numPages, tempDM.getNumPages(phonyTableId));

        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);

        return hf;
    }

    public static HeapFile createHeapFile(int numCols, int numPages) {
        return createHeapFile("empty", numCols, numPages);
    }

    public static HeapFile createHeapFile(int numCols) {
        return createHeapFile(numCols, 1);
    }

    /**
     * Creates heap file with numPages where all pages full except last which
     * just has one tuple on it.
     * @param numCols number of columns for each tuple
     * @param numPages number of pages
     * @return HeapFile
     */
    public static HeapFile createFullHeapFile(int numCols, int numPages) {
        TupleDesc td = TestUtility.getTupleDesc(numCols);
        int pageSize = Database.getPageSize();
        File emptyFile;
        try {
            emptyFile = File.createTempFile("empty", ".dat");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        emptyFile.deleteOnExit();

        int phonyTableId = -1; // temporary tableid...  the real one created when table added to catalog
        DiskManagerImpl tempDM = new DiskManagerImpl(pageSize);
        tempDM.addFileEntry(phonyTableId, emptyFile.getAbsolutePath());
        PageMaker pm = new SlottedPageMaker(td, pageSize);

        for (int pageNo = 0; pageNo < numPages; pageNo++) {
            SimplePageId pid = new SimplePageId(phonyTableId, pageNo);
            tempDM.allocatePage(pid);
            SlottedPage page = (SlottedPage) pm.makePage(pid);
            int numSlots = page.getNumSlots();
            if (pageNo == numPages - 1) {
                numSlots = 1;
            }
            for (int slotNo = 0; slotNo < numSlots; slotNo++) {
                page.insertTuple(TestUtility.getIntTuple(slotNo, numCols));
            }
            if (pageNo != numPages - 1) {
                assertEquals(0, page.getNumEmptySlots());
            }
            tempDM.writePage(page);
        }

        assertEquals(numPages, tempDM.getNumPages(phonyTableId));

        String tableName = "emptyTable";
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);
        return hf;
    }

    public static HeapFile createHeapFile(int numCols, int[] tuples) {
        assert tuples.length % numCols == 0;
        int numTuples = tuples.length / numCols;
        TupleIterator tupleList = createTupleList(numCols, tuples);
        TupleDesc td = TestUtility.getTupleDesc(numCols);
        int pageSize = Database.getPageSize();
        File emptyFile;
        try {
            emptyFile = File.createTempFile("empty", ".dat");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        emptyFile.deleteOnExit();

        int phonyTableId = -1; // temporary tableid...  the real one created when table added to catalog
        DiskManagerImpl tempDM = new DiskManagerImpl(pageSize);
        tempDM.addFileEntry(phonyTableId, emptyFile.getAbsolutePath());
        PageMaker pm = new SlottedPageMaker(td, pageSize);

        int pageNo = 0;
        // create first page
        SimplePageId pid = new SimplePageId(phonyTableId, pageNo);
        tempDM.allocatePage(pid);
        SlottedPage page = (SlottedPage) pm.makePage(pid);

        while (tupleList.hasNext()) {
            Tuple tuple = tupleList.next();
            if (page.getNumEmptySlots() == 0) {
                tempDM.writePage(page);
                pageNo++;
                pid = new SimplePageId(phonyTableId, pageNo);
                tempDM.allocatePage(pid);
                page = (SlottedPage) pm.makePage(pid);
            }
            assertTrue(page.getNumEmptySlots() > 0);
            page.insertTuple(tuple);
        }
        tupleList.close();

        tempDM.writePage(page);
        int numPages = pageNo+1;

        // fetch all tuples that we just added...
        List<Tuple> tuplesOnDisk = new LinkedList<Tuple>();
        for (pageNo = 0; pageNo < numPages; pageNo++) {
            pid = new SimplePageId(phonyTableId, pageNo);
            page = (SlottedPage) tempDM.readPage(pid, pm);
            Iterator<Tuple> iterator = page.iterator();
            while (iterator.hasNext()) {
                Tuple next = iterator.next();
                tuplesOnDisk.add(next);
            }
        }
        //... and check that all the data is there
        assertEquals(numTuples, tuplesOnDisk.size());
        tupleList.open();
        Iterator<Tuple> tuplesOnDiskIterator = tuplesOnDisk.iterator();
        while (tupleList.hasNext()) {
            Tuple origTuple = tupleList.next();
            assertTrue(tuplesOnDiskIterator.hasNext());
            Tuple tuple = tuplesOnDiskIterator.next();
            assertEqualTuples(origTuple, tuple);
        }
        assertFalse(tuplesOnDiskIterator.hasNext());

        String tableName = "emptyTable";
        HeapFile hf = Catalog.addHeapFile(tableName, td, emptyFile);
        return hf;
    }

    /**
     * @param width   the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *                in slot j * width + i
     * @return a DbIterator over a list of tuples constructed over the data
     * provided in the constructor. This iterator is already open.
     * @throws DbException if we encounter an error creating the
     *                     TupleIterator
     * @require tupdata.length % width == 0
     */
    public static TupleIterator createTupleList(int width, int[] tupdata) {
        int i = 0;
        ArrayList<Tuple> tuplist = new ArrayList<Tuple>();
        while (i < tupdata.length) {
            Tuple tup = new Tuple(TestUtility.getTupleDesc(width));
            for (int j = 0; j < width; ++j)
                tup.setField(j, new IntField(tupdata[i++]));
            tuplist.add(tup);
        }

        TupleIterator result = new TupleIterator(TestUtility.getTupleDesc(width), tuplist);
        result.open();
        return result;
    }


    /** Opens a HeapFile and adds it to the catalog.
     *
     * @param cols number of columns in the table.
     * @param f location of the file storing the table.
     * @return the opened table.
     */
    public static HeapFile openHeapFile(int cols, File f) {
        return openHeapFile(cols, "", f);
    }

    public static HeapFile openHeapFile(int cols, String colPrefix, File f) {
        // create the HeapFile and add it to the catalog
        TupleDesc td = TestUtility.getTupleDesc(cols, colPrefix);
        String tableName = "emptyTable";
        HeapFile hf = Catalog.addHeapFile(tableName, td, f);
        return hf;
    }
}
