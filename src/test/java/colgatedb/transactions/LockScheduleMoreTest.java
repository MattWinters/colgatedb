package colgatedb.transactions;

import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

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
public class LockScheduleMoreTest {
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private SimplePageId pid4 = new SimplePageId(0, 4);
    private LockManager lm;
    private Schedule.Step[] steps;
    private Schedule schedule;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="20.1", max_score=0.25, visibility="after_due_date")
    public void twoSimultaneousSharedLocks() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
        };
        executeSchedule();
    }

    @Test
    @GradedTest(number="20.2", max_score=0.25, visibility="after_due_date")
    public void waitForExclusive() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
        };
        executeSchedule();
    }

    @Test
    @GradedTest(number="20.3", max_score=0.25, visibility="after_due_date")
    public void waitYourTurn() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid2, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid2, pid1, Schedule.Action.UNLOCK)
        };
        executeSchedule();
    }

    @Test
    @GradedTest(number="20.4", max_score=0.25, visibility="after_due_date")
    public void multipleQueuedSharedRequestsSimpler() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),
                new Schedule.Step(tid2, pid1, Schedule.Action.SHARED),
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
//                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),
                new Schedule.AcquiredStep(tid2, pid1, 4),
        };
        executeSchedule();
    }


    @Test
    @GradedTest(number="20.5", max_score=0.25, visibility="after_due_date")
    public void multipleQueuedSharedRequests() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // step 0
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // step 1
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),     // step 2
                new Schedule.Step(tid2, pid1, Schedule.Action.SHARED),     // step 3
                new Schedule.Step(tid3, pid1, Schedule.Action.SHARED),     // step 4
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),     // step 5: all queued requests should be granted
                new Schedule.AcquiredStep(tid1, pid1),     // acquired in previous step
                new Schedule.AcquiredStep(tid2, pid1, 5),  // acquired in step 5
                new Schedule.AcquiredStep(tid3, pid1, 5),  // acquired in step 5
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
