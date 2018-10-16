package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Field;
import colgatedb.tuple.Op;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.*;

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
 * OrderBy is an operator that implements a relational ORDER BY.
 */
public class OrderBy extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TupleDesc td;
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private int orderByField;
    private String orderByFieldName;
    private Iterator<Tuple> it;
    private boolean asc;
    private boolean open;

    /**
     * Creates a new OrderBy node over the tuples from the iterator.
     *
     * @param orderbyField the field to which the sort is applied.
     * @param asc          true if the sort order is ascending.
     * @param child        the tuples to sort.
     */
    public OrderBy(int orderbyField, boolean asc, DbIterator child) {
        this.child = child;
        td = child.getTupleDesc();
        this.orderByField = orderbyField;
        this.orderByFieldName = td.getFieldName(orderbyField);
        this.asc = asc;
    }

    public boolean isASC() {
        return this.asc;
    }

    public int getOrderByField() {
        return this.orderByField;
    }

    public String getOrderFieldName() {
        return this.orderByFieldName;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        // load all the tuples in a collection, and sort it
        while (child.hasNext())
            childTups.add((Tuple) child.next());
        Collections.sort(childTups, new TupleComparator(orderByField, asc));
        it = childTups.iterator();
        open = true;
    }

    public void close() {
        open = false;
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = childTups.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return open && it != null && it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("no more tuples!");
        }
        return it.next();
    }


    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}

class TupleComparator implements Comparator<Tuple> {
    int field;
    boolean asc;

    public TupleComparator(int field, boolean asc) {
        this.field = field;
        this.asc = asc;
    }

    public int compare(Tuple o1, Tuple o2) {
        Field t1 = (o1).getField(field);
        Field t2 = (o2).getField(field);
        if (t1.compare(Op.EQUALS, t2))
            return 0;
        if (t1.compare(Op.GREATER_THAN, t2))
            return asc ? 1 : -1;
        else
            return asc ? -1 : 1;
    }

}
