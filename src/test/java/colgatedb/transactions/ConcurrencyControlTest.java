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
public class ConcurrencyControlTest {
    private SimplePageId pid = new SimplePageId(0, 1);
    private LockManager lm;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="18.1", max_score=1.0, visibility="visible")
    public void runOneThread() throws TransactionAbortedException, InterruptedException {
        int numThreads = 1;
        int numAdds = 10;
        executeConcurrentThreads(numThreads, numAdds);
    }

    @Test
    @GradedTest(number="18.2", max_score=1.0, visibility="visible")
    public void runTwoThreads() throws TransactionAbortedException, InterruptedException {
        int numThreads = 2;
        int numAdds = 10;
        executeConcurrentThreads(numThreads, numAdds);
    }

    @Test
    @GradedTest(number="18.3", max_score=1.0, visibility="visible")
    public void runManyThreads() throws TransactionAbortedException, InterruptedException {
        int numThreads = 20;
        int numAdds = 10;
        executeConcurrentThreads(numThreads, numAdds);
    }

    private void executeConcurrentThreads(int numThreads, int numAdds) throws InterruptedException {
        Counter counter = new Counter();
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(new Incrementer(new TransactionId(), counter, numAdds));
            threads[i] = thread;
            thread.start();
        }

        // wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }
        int expectedCount = numThreads * numAdds;
        int actualCount = counter.getCount();
        assertEquals(expectedCount, actualCount);
    }

    static class Counter {
        private int count = 0;

        public void increment(String name) {
            int currCount = count;  // read
            // introduce a delay between read and write to "encourage" race conditions
            System.out.println("Shared counter incremented by " + name + ".");
            count = currCount + 1;  // write
        }

        public int getCount() {
            return count;
        }
    }

    /**
     * Incrementer is a thread that has a reference to a shared Counter
     * object.  Once this thread starts, it increments the shared Counter
     * several times.
     */
    class Incrementer implements Runnable {

        private final TransactionId tid;
        private final Counter counter;
        private final int numIncrements;

        public Incrementer(TransactionId tid, Counter counter, int numIncrements) {
            this.tid = tid;
            this.counter = counter;
            this.numIncrements = numIncrements;
        }

        public void run() {
            try {
                // increment the counter numIncrements times
                for (int i = 0; i < numIncrements; i++) {
                    lm.acquireLock(tid, pid, Permissions.READ_WRITE);
                    counter.increment(tid.toString());
                    lm.releaseLock(tid, pid);
                }
            } catch (TransactionAbortedException e) {
                fail("Should not abort");
            }
        }
    }

}
