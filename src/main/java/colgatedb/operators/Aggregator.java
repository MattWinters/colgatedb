package colgatedb.operators;

import colgatedb.tuple.Tuple;

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
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab6, you are not required
     * to implement them until then.
     */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab6.
         */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab6.
         */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == MIN)
                return "min";
            if (this == MAX)
                return "max";
            if (this == SUM)
                return "sum";
            if (this == SUM_COUNT)
                return "sum_count";
            if (this == AVG)
                return "avg";
            if (this == COUNT)
                return "count";
            if (this == SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @see TupleIterator for a possible helper
     */
    public DbIterator iterator();

}
