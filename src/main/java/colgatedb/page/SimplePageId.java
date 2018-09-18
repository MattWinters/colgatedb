package colgatedb.page;

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
 * A simple identifier for page objects.
 */
public class SimplePageId implements PageId {

    private int tableId;
    private int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public SimplePageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int pageNumber() {
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the combination of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table, for example.)
     * <p>
     * Write a good hash function that combines table number and page number
     * in a principled way!
     */
    public int hashCode() {
        int c = 37;
        return tableId * 37 + pgNo;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
        if (o instanceof PageId){
            PageId other = (PageId) o;
            if (tableId == other.getTableId() && pgNo == other.pageNumber()){
                return true;
            }
        }
        return false;
    }

    /**
     * @return Returns a string that is "x-y" where x is the tableId and y is the page number.
     */
    public String toString() {
        return tableId + "-" + pgNo;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
