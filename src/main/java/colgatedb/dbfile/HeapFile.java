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

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public TupleDesc getTupleDesc() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {

        public HeapFileIterator(TransactionId tid) {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void open() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            throw new UnsupportedOperationException("implement me!");
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("implement me!");
        }
    }

}
