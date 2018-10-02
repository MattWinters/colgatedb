package colgatedb.transactions;

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
 * Exception that is thrown when a transaction has aborted.
 */
public class TransactionAbortedException extends Exception {
    private static final long serialVersionUID = 1L;

    public TransactionAbortedException() {
    }
}
