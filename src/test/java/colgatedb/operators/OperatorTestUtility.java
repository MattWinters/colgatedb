package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.TestUtility;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.*;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static colgatedb.page.PageTestUtility.compareTuples;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
public class OperatorTestUtility {


    /**
     * @param width   the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *                in slot j * width + i
     * @return a DbIterator over a list of tuples constructed over the data
     * provided in the constructor. This iterator is already open.
     * @throws DbException if we encounter an error creating the
     *                     TupleIterator
     * @require tupdata.length % width == 0
     */
    public static TupleIterator createTupleList(int width, int[] tupdata) {
        int i = 0;
        ArrayList<Tuple> tuplist = new ArrayList<Tuple>();
        while (i < tupdata.length) {
            Tuple tup = new Tuple(TestUtility.getTupleDesc(width));
            for (int j = 0; j < width; ++j)
                tup.setField(j, getField(tupdata[i++]));
            tuplist.add(tup);
        }

        TupleIterator result = new TupleIterator(TestUtility.getTupleDesc(width), tuplist);
        result.open();
        return result;
    }

    /**
     * @param width   the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *                in slot j * width + i.  Objects can be strings or ints;  tuples must all be of same type.
     * @return a DbIterator over a list of tuples constructed over the data
     * provided in the constructor. This iterator is already open.
     * @throws DbException if we encounter an error creating the
     *                     TupleIterator
     * @require tupdata.length % width == 0
     */
    public static TupleIterator createTupleList(int width, Object[] tupdata) {
        ArrayList<Tuple> tuplist = new ArrayList<Tuple>();
        TupleDesc td;
        Type[] types = new Type[width];
        int i = 0;
        for (int j = 0; j < width; j++) {
            if (tupdata[j] instanceof String) {
                types[j] = Type.STRING_TYPE;
            }
            if (tupdata[j] instanceof Integer) {
                types[j] = Type.INT_TYPE;
            }
        }
        td = new TupleDesc(types);

        while (i < tupdata.length) {
            Tuple tup = new Tuple(td);
            for (int j = 0; j < width; j++) {
                Field f;
                Object t = tupdata[i++];
                if (t instanceof String)
                    f = new StringField((String) t, Type.STRING_LEN);
                else
                    f = new IntField((Integer) t);

                tup.setField(j, f);
            }
            tuplist.add(tup);
        }

        TupleIterator result = new TupleIterator(td, tuplist);
        result.open();
        return result;
    }

    /**
     * Check to see if every tuple in expected matches <b>some</b> tuple
     * in actual via compareTuples. Note that actual may be a superset.
     * If not, throw an assertion.
     */
    public static void matchAllTuples(DbIterator expected, DbIterator actual) throws
            DbException, TransactionAbortedException {
        boolean matched = false;
        while (expected.hasNext()) {
            Tuple expectedTup = expected.next();
            matched = false;
            actual.rewind();

            while (actual.hasNext()) {
                Tuple next = actual.next();
                if (compareTuples(expectedTup, next)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw new RuntimeException("expected tuple not found: " + expectedTup);
            }
        }
    }

    /**
     * @return an IntField with value n
     */
    public static Field getField(int n) {
        return new IntField(n);
    }

    /**
     * Check to see if the DbIterators have the same number of tuples and
     * each tuple pair in parallel iteration satisfies compareTuples .
     * If not, throw an assertion.
     */
    public static void compareDbIterators(DbIterator expected, DbIterator actual)
            throws DbException, TransactionAbortedException {
        while (expected.hasNext()) {
            assertTrue(actual.hasNext());

            Tuple expectedTup = expected.next();
            Tuple actualTup = actual.next();
            assertTrue(compareTuples(expectedTup, actualTup));
        }
        // Both must now be exhausted
        assertFalse(expected.hasNext());
        assertFalse(actual.hasNext());
    }

    /**
     * Verifies that the DbIterator has been exhausted of all elements.
     */
    public static boolean checkExhausted(DbIterator it)
            throws TransactionAbortedException, DbException {

        if (it.hasNext()) return false;

        try {
            Tuple t = it.next();
            System.out.println("Got unexpected tuple: " + t);
            return false;
        } catch (NoSuchElementException e) {
            return true;
        }
    }

    /**
     * Mock SeqScan class for unit testing.
     */
    public static class MockScan implements DbIterator {
        private int cur, low, high, width;

        /**
         * Creates a fake SeqScan that returns tuples sequentially with 'width'
         * fields, each with the same value, that increases from low (inclusive)
         * and high (exclusive) over getNext calls.
         */
        public MockScan(int low, int high, int width) {
            this.low = low;
            this.high = high;
            this.width = width;
            this.cur = low;
        }

        public void open() {
        }

        public void close() {
        }

        public void rewind() {
            cur = low;
        }

        public TupleDesc getTupleDesc() {
            return TestUtility.getTupleDesc(width);
        }

        protected Tuple readNext() {
            if (cur >= high) return null;

            Tuple tup = new Tuple(getTupleDesc());
            for (int i = 0; i < width; ++i)
                tup.setField(i, new IntField(cur));
            cur++;
            return tup;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (cur >= high) return false;
            return true;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (cur >= high) throw new NoSuchElementException();
            Tuple tup = new Tuple(getTupleDesc());
            for (int i = 0; i < width; ++i)
                tup.setField(i, new IntField(cur));
            cur++;
            return tup;
        }
    }

}

