package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
public class Schedule {

    private static final int STEP_DELAY = 10;  // how long scheduler waits before moving on to next step
    private Step[] schedule;
    // locksAcquired holds for each txn, map of pages locked and timestamp at which lock occurred
    private Map<TransactionId, Map<PageId, Integer>> locksAcquired = new HashMap<>();
    private final LockManager lm;
    private int stepIdx = 0;

    /**
     * Simulates running the given schedule.  A schedule is a sequence of steps.  Each step
     * corresponds to a particular transaction taking a particular action (requesting a lock,
     * acquiring a lock, releasing a lock).  There is a scheduler thread that moves down the
     * schedule, ensuring that each action is initiated.
     *
     * Each transaction mentioned in the schedule is executed in a separate thread but each
     * transaction thread "waits its turn" and only takes an action when the scheduler thread
     * tells it to.
     *
     * @param schedule
     * @param lm
     * @throws InterruptedException
     */
    public Schedule(Step[] schedule, LockManager lm) throws InterruptedException {
        this.schedule = schedule;
        this.lm = lm;
        Set<TransactionId> tids = new HashSet<>();
        for (Step step : schedule) {
            tids.add(step.tid);
        }
        for (TransactionId tid : tids) {
            locksAcquired.put(tid, new HashMap<>());
            new Thread(new TxnThread(tid)).start();
        }
        Thread scheduler = new Thread(new ScheduleThread());
        scheduler.start();
        scheduler.join();  // pause current thread until scheduler is done; don't wait for txn threads as they may wait forever
    }

    /**
     * @return true is every step in schedule is completed without error
     */
    public synchronized boolean allStepsCompleted() {
        for (int i = 0; i < schedule.length; i++) {
            Step step = schedule[i];
            if (step.error != null) {
                System.err.println("An error occurred in step " + i + " of schedule.");
                step.error.printStackTrace();
                return false;
            }
            if (!step.isCompleted) {
                System.err.println("Step " + i + " of schedule was never completed.");
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if we've reached the end of the schedule
     */
    public synchronized boolean isDone() {
        return stepIdx >= schedule.length;
    }

    /**
     * Advances to the next step in the schedule.  Issues a warning if the current step has not completed.  Note
     * that this happens when a txn is waiting for a lock which is not released until a later step.
     */
    public synchronized void advance() {
        if (!schedule[stepIdx].isCompleted) {
            System.err.println("Current step " + stepIdx + " is not yet complete; scheduler advancing anyway.");
        }
        if (stepIdx < schedule.length) {
            stepIdx++;
        }
        notifyAll();
    }

    /**
     * Executes one step in the schedule.  This is run at every step by every transaction.  The first thing the txn
     * does is check whether this is one of their steps.  If it's not, then the txn waits until the scheduler moves
     * to the next step.  If it is, then the txn thread takes the appropriate action.
     *
     * Note: if the action is acquiring a lock, the txn may end up waiting for the lock and thus this step may not
     * get completed until some later point.
     * @param tid
     */
    public void takeAction(TransactionId tid) {
        Step step;
        Permissions perm = null;
        boolean isMyTurn;
        synchronized (this) {
            step = schedule[this.stepIdx];
            isMyTurn = !isDone() && step.tid.equals(tid) && !step.isStarted;
            if (!isMyTurn) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Taking action " + this.stepIdx + ": " + step);
                step.isStarted = true;
                try {
                    switch (step.action) {
                        case SHARED:
                            perm = Permissions.READ_ONLY; // don't attempt to acquire lock yet...  see below
                            break;
                        case EXCLUSIVE:
                            perm = Permissions.READ_WRITE; // don't attempt to acquire lock yet...  see below
                            break;
                        case UNLOCK:
                            checkLockState(tid, step.pid, true);
                            lm.releaseLock(tid, step.pid);
                            locksAcquired.get(tid).remove(step.pid);
                            checkLockState(tid, step.pid, false);
                            step.complete();
                            break;
                        case ACQUIRED:
                            checkLockState(tid, step.pid, true);
                            // check when the lock was acquired
                            if (locksAcquired.get(tid).containsKey(step.pid)) {
                                Integer stepWhenAcquired = locksAcquired.get(tid).get(step.pid);
                                // lock should have been acquired in previous step
                                int whenExpectedToAcquire = this.stepIdx - 1;
                                if (step instanceof AcquiredStep) {
                                    int temp = ((AcquiredStep)step).whenAcquired;
                                    if (temp != -1) {
                                        whenExpectedToAcquire = temp;
                                    }
                                }
                                if (stepWhenAcquired != whenExpectedToAcquire) {
                                    throw new ScheduleException("Lock acquired too early!");
                                }
                                step.complete();
                            } else {
                                throw new ScheduleException("Lock not acquired!");
                            }
                    }
                } catch (ScheduleException e) {
                    step.error = e;
                }
            }
        }


        // check if this step is a lock acquisition step
        // this is done outside synchronized block because these actions may cause thread to pause
        // and we don't want this thread to be holding any locks on the scheduler if it does pause
        if (perm != null) {
            try {
                lm.acquireLock(tid, step.pid, perm);

                // now make any updates to schedule state, e.g., record the step at which lock finally acquired
                synchronized (this) {
                    step.complete();
                    locksAcquired.get(tid).put(step.pid, stepIdx);
                    checkLockState(tid, step.pid, true);
                }
            } catch (TransactionAbortedException | ScheduleException e) {
                step.error = e;
            }
        }
    }

    /**
     * Checks that the given transaction has a lock if it should and doesn't have a lock if it shouldn't.
     * @param tid txn whose locks are being checked
     * @param pid the page being locked
     * @param expected true if you expect txn to hold lock, false otherwise
     */
    private void checkLockState(TransactionId tid, PageId pid, boolean expected) {
        boolean hasLock = lm.holdsLock(tid, pid, Permissions.READ_ONLY);
        if (hasLock && !expected) {
            throw new ScheduleException("should not have the lock, but does!");
        } else if (!hasLock && expected) {
            throw new ScheduleException("does not have the lock, but should!");
        }

    }

    /**
     * Represents a single step in the schedule.
     */
    public static class Step {
        TransactionId tid;
        PageId pid;
        Action action;
        boolean isStarted;
        boolean isCompleted;
        Exception error;

        public Step(TransactionId tid, PageId pid, Action action) {
            this.tid = tid;
            this.pid = pid;
            this.action = action;
            isStarted = false;
            isCompleted = false;
        }

        public String toString() {
            return "Step(" + tid + " action=" + action + ")";
        }

        public void complete() {
            isCompleted = true;
        }
    }

    public static class AcquiredStep extends Step {
        int whenAcquired = -1;   // on which step in schedule the lock is acquired (-1 to indicate previous step)

        public AcquiredStep(TransactionId tid, PageId pid) {
            this(tid, pid, -1);
        }
        public AcquiredStep(TransactionId tid, PageId pid, int whenAcquired) {
            super(tid, pid, Action.ACQUIRED);
            this.whenAcquired = whenAcquired;
        }
    }

    public enum Action {
        SHARED,          // request shared lock
        EXCLUSIVE,       // request exclusive lock
        ACQUIRED,        // check that lock was acquired in this step or immediately preceding step
        UNLOCK           // release the lock
    };

    /**
     * Simulates a transaction
     */
    class TxnThread implements Runnable {
        private final TransactionId tid;

        public TxnThread(TransactionId tid) {
            this.tid = tid;
        }
        @Override
        public void run() {
            while (!isDone()) {
                takeAction(tid);
            }
        }
    }

    /**
     * This thread responsible for advancing the schedule (after a reasonable delay to give txns a chance to
     * execute their action).
     */
    class ScheduleThread implements Runnable {
        @Override
        public void run() {
            while (!isDone()) {
                try {
                    Thread.sleep(STEP_DELAY);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                advance();
            }
        }
    }



}
