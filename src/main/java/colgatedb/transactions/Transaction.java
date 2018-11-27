package colgatedb.transactions;

import colgatedb.Database;

import java.io.IOException;

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
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */
public class Transaction {
    private final TransactionId tid;
    volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    /** Start the transaction running */
    public void start() {
        started = true;
        try {
            Database.getLogFile().logXactionBegin(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TransactionId getId() {
        return tid;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(true);
    }

    /** Finish the transaction */
    public void abort() throws IOException {
        transactionComplete(false);
    }

    /** Handle the details of transaction commit / abort */
    private void transactionComplete(boolean commit) throws IOException {

        if (started) {
            if (commit) {
                Database.getLogFile().logCommit(tid);
            } else {
                Database.getLogFile().logAbort(tid); //does rollback too
            }
            Database.getAccessManager().transactionComplete(tid, commit);
        } else {
            throw new RuntimeException("Txn was never started!");
        }
    }
}
