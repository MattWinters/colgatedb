package colgatedb.logging;

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

public class LogType {
    public static final int ABORT_RECORD = 1;
    public static final int COMMIT_RECORD = 2;
    public static final int UPDATE_RECORD = 3;
    public static final int BEGIN_RECORD = 4;
    public static final int CHECKPOINT_RECORD = 5;
    public static final int CLR_RECORD = 6;
}
