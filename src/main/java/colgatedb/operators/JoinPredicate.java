package colgatedb.operators;

import colgatedb.tuple.Op;
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
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {


    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Op); either
     *               Op.GREATER_THAN, Op.LESS_THAN,
     *               Op.EQUAL, Op.GREATER_THAN_OR_EQ, or
     *               Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Op op, int field2) {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        throw new UnsupportedOperationException("implement me!");
    }

    public int getField1() {
        throw new UnsupportedOperationException("implement me!");
    }

    public int getField2() {
        throw new UnsupportedOperationException("implement me!");
    }

    public Op getOperator() {
        throw new UnsupportedOperationException("implement me!");
    }
}
