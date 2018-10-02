package colgatedb.transactions;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

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
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    static AtomicLong counter = new AtomicLong(0);
    final long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object tid) {
        return (tid instanceof TransactionId) && ((TransactionId) tid).myid == myid;
    }

    public int hashCode() {
        return (int) myid;
    }

    public String toString() {
        return "tid=" + myid;
    }
}
