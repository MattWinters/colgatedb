package colgatedb.tuple;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;

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
 * Class representing a type in ColgateDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                return new IntField(dis.readInt());
            } catch (IOException e) {
                throw new RuntimeException("Error reading from stream", new ParseException("couldn't parse", 0));
            }
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN - strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new RuntimeException("Error reading from stream", new ParseException("couldn't parse", 0));
            }
        }
    };

    public static final int STRING_LEN = 128;

    /**
     * @return the number of bytes required to store a field of this type.
     */
    public abstract int getLen();

    /**
     * @param dis The input stream to read from
     * @return a Field object of the same type as this object that has contents
     * read from the specified DataInputStream.
     * @throws RuntimeException if the data read from the input stream is not
     *                        of the appropriate type.
     */
    public abstract Field parse(DataInputStream dis);

}
