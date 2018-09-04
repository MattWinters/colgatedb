package colgatedb.tuple;

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

/** Constants used for return codes in Field.compare */
public enum Op implements Serializable {
    EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

    /**
     * Interface to access operations by integer value for command-line
     * convenience.
     *
     * @param i
     *            a valid integer Op index
     */
    public static Op getOp(int i) {
        return values()[i];
    }

    public String toString() {
        if (this == EQUALS)
            return "=";
        if (this == GREATER_THAN)
            return ">";
        if (this == LESS_THAN)
            return "<";
        if (this == LESS_THAN_OR_EQ)
            return "<=";
        if (this == GREATER_THAN_OR_EQ)
            return ">=";
        if (this == LIKE)
            return "LIKE";
        if (this == NOT_EQUALS)
            return "<>";
        throw new IllegalStateException("impossible to reach here");
    }

}
