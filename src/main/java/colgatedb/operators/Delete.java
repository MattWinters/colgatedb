package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * @return tuple desc of the insert operator should be a single INT named count
     */
    @Override
    public TupleDesc getTupleDesc() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     *
     * @return true if this is the first time being called...  even if child is empty,
     *         this iterator still has one tuple to return (the tuple that says that zero
     *         records were deleted).
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the deleteTuple method on the appropriate DbFile.  The
     * DbFile can be obtained via a combination of the RecordId of the tuple
     * being deleted and the Catalog.
     *
     * @return A single-field tuple containing the number of deleted records.
     * @throws NoSuchElementException if called more than once
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public DbIterator[] getChildren() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void setChildren(DbIterator[] children) {
        throw new UnsupportedOperationException("implement me!");
    }

}
