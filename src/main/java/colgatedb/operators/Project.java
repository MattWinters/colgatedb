package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import java.util.ArrayList;
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
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

    private static final long serialVersionUID = 1L;
    private boolean open;
    private DbIterator child;
    private TupleDesc td;
    private ArrayList<Integer> outFieldIds;

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     *
     * @param fieldList The ids of the fields child's tupleDesc to project out
     * @param typesList the types of the fields in the final projection
     * @param child     The child operator
     */
    public Project(ArrayList<Integer> fieldList, ArrayList<Type> typesList,
                   DbIterator child) {
        this(fieldList, typesList.toArray(new Type[typesList.size()]), child);
    }

    public Project(ArrayList<Integer> fieldList, Type[] types,
                   DbIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childtd = child.getTupleDesc();

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childtd.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(types, fieldAr);
        open = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        open = true;
    }

    public void close() {
        child.close();
        open = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        return open && child.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException  {
        if (!hasNext()) {
            throw new NoSuchElementException("no more tuples!");
        }
        Tuple t = child.next();
        Tuple newTuple = new Tuple(td);
        newTuple.setRecordId(t.getRecordId());
        for (int i = 0; i < td.numFields(); i++) {
            newTuple.setField(i, t.getField(outFieldIds.get(i)));
        }
        return newTuple;
    }


    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        child = children[0];
    }

}
