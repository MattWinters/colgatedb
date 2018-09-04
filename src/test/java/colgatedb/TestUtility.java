package colgatedb;

import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import java.util.ArrayList;

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
public class TestUtility {
    /**
     * @return a Type array of length len populated with Type.INT_TYPE
     */
    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i)
            types[i] = Type.INT_TYPE;
        return types;
    }

    /**
     * @return a String array of length len populated with the (possibly null) strings in val,
     * and an appended increasing integer at the end (val0, val1, etc.).
     */
    public static String[] getStrings(int len, String val) {
        String[] strings = new String[len];
        for (int i = 0; i < len; ++i) {
            strings[i] = val + i;
        }
        return strings;
    }

    /**
     * @return a TupleDesc with n fields of type Type.INT_TYPE, each named
     * name + n (name0, name1, name2, etc.).
     */
    public static TupleDesc getTupleDesc(int n, String name) {
        return new TupleDesc(getTypes(n), getStrings(n, name));
    }

    /**
     * @return a TupleDesc with n fields of type Type.INT_TYPE
     */
    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

    /**
     * @param values values to assign to columns in tuple
     * @return a tuple with values.length fields of Type.INT_TYPE
     */
    public static Tuple getIntTuple(int[] values) {
        Tuple tup = new Tuple(getTupleDesc(values.length));
        for (int i = 0; i < values.length; ++i)
            tup.setField(i, new IntField(values[i]));
        return tup;
    }

    /**
     *
     * @param n number of columns
     * @return a tuple with n fields of Type.INT_TYPE all equal to zero.
     */
    public static Tuple getIntTuple(int n) {
        return getIntTuple(new int[n]);
    }

    /**
     *
     * @param n number of columns
     * @return a tuple with n fields of Type.INT_TYPE all equal to val.
     */
    public static Tuple getIntTuple(int val, int n) {
        int[] values = new int[n];
        for (int i = 0; i < values.length; i++) {
            values[i] = val;
        }
        return getIntTuple(values);
    }

    public static Tuple getIntTuple(TupleDesc td, int[] tupdata) {
        Tuple tup = new Tuple(td);
        for (int i = 0; i < tupdata.length; ++i)
            tup.setField(i, new IntField(tupdata[i]));
        return tup;
    }


    public static String listToString(ArrayList<Integer> list) {
        String out = "";
        for (Integer i : list) {
            if (out.length() > 0) out += "\t";
            out += i;
        }
        return out;
    }


}
