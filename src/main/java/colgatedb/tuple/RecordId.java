package colgatedb.tuple;

import colgatedb.page.PageId;

import java.io.Serializable;

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
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private PageId pid;
    private int tupleno;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
         return  tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
         if (o instanceof RecordId){
             RecordId other = (RecordId)o;
             if (other.tupleno() == tupleno && other.getPageId().equals(pid)){
                 return true;
             }
         }
         return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * <p>
     * Write a good hash function that combines tuple number and page id's hashcode
     * in a principled way!
     * <p>
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        int c = 23;
        return tupleno * c + pid.hashCode();

    }

}
