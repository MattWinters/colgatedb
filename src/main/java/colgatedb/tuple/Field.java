package colgatedb.tuple;

import java.io.DataOutputStream;
import java.io.IOException;
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
 * Interface for values of fields in tuples in ColgateDB.
 */
public interface Field extends Serializable {
    /**
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     *
     * @param dos The DataOutputStream to write to.
     * @see DataOutputStream
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     *
     * @param op    The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    public boolean compare(Op op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     *
     * @return type of this field
     */
    public Type getType();

    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    public int hashCode();

    public boolean equals(Object field);

    public String toString();
}
