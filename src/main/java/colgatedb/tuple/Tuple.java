package colgatedb.tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
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
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     * @throws RuntimeException if f does not match type of field i.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public void setField(int i, Field f) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Field getField(int i) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * <p>
     * where \t is a tab and \n is a newline
     */
    public String toString() {
        throw new UnsupportedOperationException("implement me!");
    }


    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // hint: use java.util.Arrays.asList to convert array into a list, then return list iterator.
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
         // you do not need to implement for lab 1
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
         // you do not need to implement for lab 1
        throw new UnsupportedOperationException("implement me!");
    }
}
