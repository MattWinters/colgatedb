package colgatedb.dbfile;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;

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
 * DbFileIterator is the iterator interface that all ColgateDB Dbfile should
 * implement.
 */
public interface DbFileIterator {

    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
            throws TransactionAbortedException;

    /**
     * @return true if there are more tuples available.
     */
    public boolean hasNext()
            throws TransactionAbortedException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
            throws TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws TransactionAbortedException;

    /**
     * Closes the iterator.
     */
    public void close();
}
