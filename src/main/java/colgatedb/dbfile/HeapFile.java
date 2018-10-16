package colgatedb.dbfile;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Iterator;
import java.util.NoSuchElementException;

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

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor
    private int numPages;
    private int pageSize;
    private int tableid;
    private TupleDesc td;
    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.numPages = numPages;
        this.pageSize = pageSize;
        this.td = td;
        this.tableid = tableid;
        this.pageMaker = new SlottedPageMaker(td, pageSize);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    @Override
    public int getId() {
        return tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    public PageId findOrCreateEmptySlot (){
        //Loop though every page to check it has an empty slot
        for (int pageNum = 0; pageNum < numPages; pageNum++){
            PageId pid = new SimplePageId(tableid, pageNum);
            SlottedPage page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
            for (int slot = 0; slot < page.getNumSlots(); slot ++){
                if (page.isSlotEmpty(slot)){
                    Database.getBufferManager().unpinPage(pid, false);
                    return (pid);
                }
            }
            Database.getBufferManager().unpinPage(pid, false);
        }
        PageId pid = new SimplePageId(tableid, numPages );
        Database.getBufferManager().allocatePage(pid);
        numPages ++;
//        Database.getBufferManager().pinPage(pid, pageMaker);
//        Database.getBufferManager().unpinPage(pid, false);
        return pid;
       }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        PageId pid = findOrCreateEmptySlot();
        SlottedPage page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
        // Go through the slots of the page and find the empty slot
        for (int slot = 0; slot < page.getNumSlots(); slot ++) {
            if (page.isSlotEmpty(slot)) {
                page.insertTuple(slot, t);
                break;
            }
        }
        Database.getBufferManager().unpinPage(pid, true);
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        SlottedPage page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
        page.deleteTuple(t);
        Database.getBufferManager().unpinPage(pid, true);
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        int currPage;
        PageId pid;
        SlottedPage page;
        Iterator<Tuple> iterator;
        TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            currPage = 0;
            pid = new SimplePageId(tableid, currPage);
            page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
            Database.getBufferManager().unpinPage(pid, false);
            iterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (iterator == null){
                return false;
            }
            if (iterator.hasNext()) {
                return true;
            }
            else {
                currPage ++;
                if (currPage < numPages) {
                    pid = new SimplePageId(tableid, currPage);
                    page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
                    iterator = page.iterator();
                    Database.getBufferManager().unpinPage(pid, false);
                    return hasNext();

                }
            }
            return false;
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!hasNext()){
                throw new TransactionAbortedException();
            }
            else{
                Tuple nextTuple = iterator.next();
                return nextTuple;
            }
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            currPage = 0;
            pid = new SimplePageId(tableid, currPage);
            page = (SlottedPage) Database.getBufferManager().pinPage(pid, pageMaker);
            iterator = page.iterator();
            Database.getBufferManager().unpinPage(pid, false);
        }

        @Override
        public void close() {
            page = null;
            pid = null;
            iterator = null;
        }
    }

}
