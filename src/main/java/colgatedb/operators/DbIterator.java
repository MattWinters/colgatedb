package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * DbIterator is the iterator interface that all ColgateDB operators should
 * implement. In addition to any resource allocation/deallocation, an open method
 * should call any child iterator open methods, and in a close method, an iterator
 * should call its children's close methods.
 */
public interface DbIterator extends Serializable {

    /**
     * Opens the iterator. This must be called before any of the other methods.
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    void open() throws DbException, TransactionAbortedException;

    /**
     * Returns true if the iterator has more tuples.
     *
     * @return true if the iterator has more tuples, false if there are no more tuples or iterator is closed
     */
    boolean hasNext() throws DbException, TransactionAbortedException;

    /**
     * Returns the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return the next tuple in the iteration.
     * @throws NoSuchElementException if there are no more tuples or iterator is closed.
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException when rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException;

    /**
     * Returns the TupleDesc associated with this DbIterator.
     *
     * @return the TupleDesc associated with this DbIterator.
     */
    public TupleDesc getTupleDesc();

    /**
     * Closes the iterator.
     */
    public void close();

}
