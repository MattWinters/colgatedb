package colgatedb.transactions;

import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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
public class LockScheduleTest {
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private SimplePageId pid4 = new SimplePageId(0, 4);
    private LockManager lm;
    private LockTableEntry tableEntry;
    private Schedule.Step[] steps;
    private Schedule schedule;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="19.1", max_score=1.0, visibility="visible")
    public void acquireLock() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                // important detail: acquired step must be included in schedule and should appear as soon as the
                // lock is acquired.  in this case, the lock is acquired immediately.
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * Tricky test case:
     * - T1 has shared lock and T2 waiting on exclusive
     * - then T1 requests upgrade, it should be granted because upgrades get highest priority
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void upgradeRequestCutsInLine() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // t1 requests upgrade, should be able to cut line
                //new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 gets exclusive ahead of t2
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED)    // now t2 can get exclusive
        };
        executeSchedule();
    }

    //Test1 checks to make sure that a shared lock on pid1 does not affect the access to pid2s lock.
    //It should not have to wait for the pid1 lock to free up.
    @Test
    public void test1() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED), //t1 acquires shared
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),  // t2 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),  // t2 acquires shared
                new Schedule.Step(tid2, pid2, Schedule.Action.EXCLUSIVE),   // t3 requests exclusive for pid2
                new Schedule.Step(tid2, pid2, Schedule.Action.ACQUIRED)   // t3 acquires exclusive without waiting
        };
        executeSchedule();
    }

    //Test2 will check to make sure that an upgrade still waits for all of the other shared locks to be released before it gets the lock upgrade to exclusive
    @Test
    public void test2() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests Shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),  //t1 gets Shared lock
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),  // t2 requests for shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),  // t2 gets Shared
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),   // t1 requests exclusive
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK), // t2 releases the lock making t1 only shared lock holder
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED)   // now t1 gets exclusive upgrade
        };
                executeSchedule();
    }

    //Test3 will test that when multiple shared locks are held, all the locks must be released before an exclusive can grab it.
    //The test also checks to make sure after a shared lock is released an exclusive can be requested by the same tid but it is not an upgrade.
    @Test
    public void test3() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),  //t1 gets shared lock
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),  // t2 request shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),  //t2 gets shared lock
                new Schedule.Step(tid2, pid1, Schedule.Action.EXCLUSIVE),  // t3 requests exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK), // t1 releases the lock to next in queue
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),   // t1 now requests exclusive, t1 already released shared so it should go to the back of the queue (no upgrade)
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK), // t2 releases the second shared lock, the exclusive locks in queue can now get lock
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED),   // now t2 gets exclusive, It couldn't until all shared were released
        };

        executeSchedule();
    }

    private void executeSchedule() {
        try {
            schedule = new Schedule(steps, lm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(schedule.allStepsCompleted());
    }
}
