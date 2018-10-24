package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.dbfile.DbFile;
import colgatedb.dbfile.DbFileIterator;
import colgatedb.dbfile.HeapFile;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
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
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFile heapFile;
    private DbFileIterator iterator;
    private TupleDesc td;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the id of the table to scan.  The actual table can be retrieved from the Catalog.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */



    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.heapFile = Database.getCatalog().getDatabaseFile(tableid);
        this.iterator = heapFile.iterator(tid);
        TupleDesc tdOriginal = heapFile.getTupleDesc();
        Type[] typeAr = new Type[tdOriginal.numFields()];
        String[] fieldAr = new String[tdOriginal.numFields()];
        for (int i = 0; i < typeAr.length; i++){
            typeAr[i] = tdOriginal.getFieldType(i);
            fieldAr[i] = tableAlias + "." + tdOriginal.getFieldName(i);
        }
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return iterator.next();
    }

    public void close() {
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iterator.rewind();
    }
}
