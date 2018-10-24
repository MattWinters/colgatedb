package colgatedb.operators;

import colgatedb.tuple.Field;
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
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int field;
    private final Op op;
    private final Field operand;

    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.
     * @param op      operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.field = field;
        switch (this.op = op) {
            case EQUALS:
                break;
            case GREATER_THAN:
                break;
            case LESS_THAN:
                break;
            case LESS_THAN_OR_EQ:
                break;
            case GREATER_THAN_OR_EQ:
                break;
            case LIKE:
                break;
            case NOT_EQUALS:
                break;
        }
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        return  field;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        Field f = t.getField(field);
        return f.compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {
        return "f = " + field + ", op = " + op + ", operand = " + operand.toString();
    }
}
