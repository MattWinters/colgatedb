package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

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
 * Abstract class for implementing operators. It handles some common functionality.
 */
public abstract class Operator implements DbIterator {

    private TupleDesc td = null;  // should be initialized in constructor
    private int estimatedCardinality = 0;  // used by query optimizer, can be ignored
    public abstract boolean hasNext() throws DbException, TransactionAbortedException;
    public abstract Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;
    // <silenstrip>
    /**
     * Returns the next Tuple in the iterator, or null if the iteration is
     * finished. Operator uses this method to implement both <code>next</code>
     * and <code>hasNext</code>.
     *
     * @return the next Tuple in the iterator, or null if the iteration is
     * finished.
     */
//    protected Tuple fetchNext() throws DbException,
//            TransactionAbortedException { return null; }
    // </silenstrip>

    /**
     * Closes this iterator. If overridden by a subclass, they should call
     * super.close() in order for Operator's internal state to be consistent.
     */
    public abstract void close();

    public abstract void open() throws DbException, TransactionAbortedException;

    /**
     * @return return the children DbIterators of this operator. If there is
     * only one child, return an array of only one element. For binary
     * operators, the order of the children is not important. But they
     * should be consistent among multiple calls.
     */
    public abstract DbIterator[] getChildren();

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is binary, children[0]
     * and children[1] should be used.
     *
     * @param children the DbIterators which are to be set as the children(child) of
     *                 this operator
     * @throws DbException if the wrong number of children is supplied for this operator
     */
    public abstract void setChildren(DbIterator[] children);

    /**
     * @return return the TupleDesc of the output tuples of this operator
     */
    public TupleDesc getTupleDesc() {
        return td;
    };

    /**
     * @return the TupleDesc of the output tuples of this operator
     */
    public void setTupleDesc(TupleDesc td) {
        this.td = td;
    }

    /**
     * @return The estimated cardinality of this operator.
     */
    public int getEstimatedCardinality() {
        return estimatedCardinality;
    }

    /**
     * @param card The estimated cardinality of this operator
     */
    protected void setEstimatedCardinality(int card) {
        estimatedCardinality = card;
    }

}
