package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.dbfile.DbFile;
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

    private  TransactionId t;
    private DbIterator child;
    private boolean open = false;
    private boolean calledHasNext = false;
    private boolean calledNext = false;
    private TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"count"});
    private Tuple tuple = new Tuple(td);

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    /**
     * @return tuple desc of the insert operator should be a single INT named count
     */
    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        open = true;
    }

    @Override
    public void close() {
        child.close();
        open = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     *
     * @return true if this is the first time being called...  even if child is empty,
     *         this iterator still has one tuple to return (the tuple that says that zero
     *         records were deleted).
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!calledHasNext) {
            calledHasNext = true;
            return true;
        }
        while (child.hasNext() && open){
            return true;
        }
        return false;
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
        int count = 0;
        if (calledNext){
            throw new NoSuchElementException();
        }
        calledNext = true;
        if (!hasNext()){
            throw new DbException("Does not have next");
        }
        while (hasNext()) {
            Tuple next = child.next();
            int tableId = next.getRecordId().getPageId().getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            file.deleteTuple(t, next);
            count ++;
        }
        tuple.setField(0, new IntField(count));
        return tuple;
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
