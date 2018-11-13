package colgatedb.transactions;

import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
public class DeadlockTest {
    private static int DELAY = 1000;
    private static final int ADDITIONAL_DELAY = 20;
    private SimplePageId pid0 = new SimplePageId(0, 0);
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private LockManager lm;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void exclusiveDeadlock() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid1, Permissions.READ_WRITE, Permissions.READ_WRITE);
        LockGrabber t2 = new LockGrabber(tid1, pid1, pid0, Permissions.READ_WRITE, Permissions.READ_WRITE);
        executeTxns(new LockGrabber[]{t1, t2});
        assertTrue( (t1.txnAborted() && t2.txnCompleted()) ||
                        (t1.txnCompleted() && t2.txnAborted())
        );
    }

    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void readWriteDeadlock() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid1, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber t2 = new LockGrabber(tid1, pid1, pid0, Permissions.READ_ONLY, Permissions.READ_WRITE);
        executeTxns(new LockGrabber[]{t1, t2});
        assertTrue( (t1.txnAborted() && t2.txnCompleted()) ||
                        (t1.txnCompleted() && t2.txnAborted())
        );
    }

    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void updgradeDeadlock() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid0, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber t2 = new LockGrabber(tid1, pid0, pid0, Permissions.READ_ONLY, Permissions.READ_WRITE);
        executeTxns(new LockGrabber[]{t1, t2});
        assertTrue( (t1.txnAborted() && t2.txnCompleted()) ||
                        (t1.txnCompleted() && t2.txnAborted())
        );
    }

    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void lockEntriesCleanedUp() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid1, Permissions.READ_WRITE, Permissions.READ_WRITE);
        LockGrabber t2 = new LockGrabber(tid1, pid1, pid0, Permissions.READ_WRITE, Permissions.READ_WRITE);
        executeTxns(new LockGrabber[]{t1, t2});
        assertTrue( (t1.txnAborted() && t2.txnCompleted()) ||
                        (t1.txnCompleted() && t2.txnAborted())
        );
        // both of the above txns are done and should have released all locks
        // provided the lock entries are cleaned up (no stale requests in the queue), then t3 should be able
        // to acquire locks on both
        LockGrabber t3 = new LockGrabber(tid3, pid0, pid1, Permissions.READ_WRITE, Permissions.READ_WRITE);
        executeTxns(new LockGrabber[]{t3});
        assertTrue(t3.txnCompleted());
    }

    /**
     * T0 gets p0, T1 gets p1, ..., T3 gets p3.  Then T0 requests p1, T1 requests p2, T2 requests p3 and T3 requests p0,
     * creating a cycle.
     * @throws InterruptedException
     */
    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void longCycle() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid1, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber t2 = new LockGrabber(tid1, pid1, pid2, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber t3 = new LockGrabber(tid2, pid2, pid3, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber t4 = new LockGrabber(tid3, pid3, pid0, Permissions.READ_ONLY, Permissions.READ_WRITE);
        LockGrabber[] grabbers = {t1, t2, t3, t4};
        executeTxns(grabbers);
        int aborts = 0;
        int completions = 0;
        for (LockGrabber grabber : grabbers) {
            aborts += grabber.txnAborted()? 1 : 0;
            completions += grabber.txnCompleted()? 1: 0;
        }
        assertTrue(aborts > 0);  // at least one txn should abort b/c there is a cycle
        assertTrue(completions > 0);
    }

    @Test
    @GradedTest(number="21.1", max_score=1.0, visibility="visible")
    public void nonDeadlock() throws InterruptedException {
        LockGrabber t1 = new LockGrabber(tid0, pid0, pid1, Permissions.READ_ONLY, Permissions.READ_ONLY);
        LockGrabber t2 = new LockGrabber(tid1, pid1, pid0, Permissions.READ_ONLY, Permissions.READ_ONLY);
        executeTxns(new LockGrabber[]{t1, t2});
        assertTrue(t1.txnCompleted() && t2.txnCompleted());
    }

    private void executeTxns(LockGrabber[] grabbers) throws InterruptedException {
        Thread[] threads = new Thread[grabbers.length];
        for (int i = 0; i < grabbers.length; i++) {
            LockGrabber grabber = grabbers[i];
            threads[i] = new Thread(grabber);
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    /**
     * Every time this is called, the delay gets a little longer.
     * @return number of milliseconds to delay
     */
    public int getDelay() {
        DELAY += ADDITIONAL_DELAY;
        return DELAY;
    }


    /**
     * LockGrabber is a thread that has a reference to a shared Counter
     * object.  Once this thread starts, it increments the shared Counter
     * several times.
     */
    class LockGrabber implements Runnable {

        private final TransactionId tid;
        private final PageId firstPid;
        private final PageId secondPid;
        private final Permissions firstPerm;
        private final Permissions secondPerm;
        private boolean exceptionThrown;
        private boolean bothLocksAcquired;


        public LockGrabber(TransactionId tid, PageId firstPid, PageId secondPid,
                           Permissions firstPerm, Permissions secondPerm) {
            this.tid = tid;
            this.firstPid = firstPid;
            this.secondPid = secondPid;
            this.firstPerm = firstPerm;
            this.secondPerm = secondPerm;
        }

        public void run() {
            try {
                System.out.println(tid + " trying to acquire " + firstPid);
                lm.acquireLock(tid, firstPid, firstPerm);
                try {
                    Thread.sleep(getDelay());
                } catch (InterruptedException ignored) {}
                System.out.println(tid + " trying to acquire " + secondPid);
                lm.acquireLock(tid, secondPid, secondPerm);
                bothLocksAcquired = true;
                lm.releaseLock(tid, firstPid);
                if (!firstPid.equals(secondPid)) {
                    lm.releaseLock(tid, secondPid);
                }
            } catch (TransactionAbortedException e) {
                System.err.println(tid + " caught exception.");
                exceptionThrown = true;
                if (lm.holdsLock(tid, firstPid, firstPerm)) {
                    lm.releaseLock(tid, firstPid);
                }
            }
        }

        public boolean txnAborted() {
            return exceptionThrown;
        }
        public boolean txnCompleted() {
            return bothLocksAcquired;
        }
    }

}
