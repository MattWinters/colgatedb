package colgatedb.logging;

import colgatedb.page.Page;
import colgatedb.transactions.TransactionId;

import java.io.IOException;

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

public interface LogFile {

    void logXactionBegin(TransactionId tid)
            throws IOException;

    void logCommit(TransactionId tid) throws IOException;

    void logAbort(TransactionId tid) throws IOException;

    void logAbort(Long tid) throws IOException;

    void logWrite(TransactionId tid, Page before,
                  Page after)
            throws LogManagerException;

    void logCLR(TransactionId tid, Page after)
                    throws IOException;

    void logCLR(Long tid, Page after)
                            throws IOException;

    void logCheckpoint() throws IOException;

    void logTruncate() throws IOException;

    void shutdown();

    void recover() throws IOException;

    void force() throws LogManagerException;
}
