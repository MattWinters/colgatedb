package colgatedb.operators;

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
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {


    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        throw new UnsupportedOperationException("implement me!");
    }

    public Predicate getPredicate() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
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

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

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
