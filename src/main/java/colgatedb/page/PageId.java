package colgatedb.page;

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
 * PageId is an interface to a specific page of a specific table.
 */
public interface PageId {

    /**
     * @return the unique tableid hashcode for this PageId
     */
    int getTableId();

    /**
     * @return the unique page number of this PageId
     */
    int pageNumber();

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in a buffer manager, for example.)
     */
    int hashCode();

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    boolean equals(Object o);

    /**
     * Return a representation of this page id object as a collection of
     * integers (used for logging)
     * <p>
     * This class MUST have a constructor that accepts n integer parameters,
     * where n is the number of integers returned in the array from serialize.
     */
    int[] serialize();
}

