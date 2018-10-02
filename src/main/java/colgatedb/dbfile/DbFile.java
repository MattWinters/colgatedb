package colgatedb.dbfile;

import colgatedb.BufferManager;
import colgatedb.DbException;
import colgatedb.DiskManagerException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

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
 * The interface for database files on disk. Each table is represented by a
 * single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer manager, rather than directly
 * by operators.
 */
public interface DbFile {

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * <p>
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @throws DbException if the tuple cannot be added
     * @throws DiskManagerException if the needed file can't be read/written
     */
    void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException;

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * <p>
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to delete.  This tuple should be updated to reflect that
     *            it is no longer stored on any page.
     * @throws DbException if the tuple cannot be deleted or is not a member
     *                     of the file
     */
    void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException;

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must access pages through the {@link BufferManager}, rather
     * than read pages directly from disk.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    DbFileIterator iterator(TransactionId tid);

    /**
     * Returns a unique ID used to identify this DbFile in the Catalog.
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    int getId();

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    TupleDesc getTupleDesc();
}
