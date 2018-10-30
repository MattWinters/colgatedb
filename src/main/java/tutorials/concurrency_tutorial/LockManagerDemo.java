package tutorials.concurrency_tutorial;

public class LockManagerDemo {
    private static final LockManager lm = new LockManager();
    public static LockManager getLockManager() { return lm; }

    /**
     * This main program spawns a bunch of Incrementer threads
     * at the same time.  All Incrementers are modifying the same
     * shared Counter object.  Since they are all running at the
     * same time, we should see some thread interference (aka
     * race conditions) as they try to modify the
     */
    public static void main(String args[]) throws InterruptedException {
        final Counter counter = new Counter();
        int numThreads = 20;
        final int numAdds = 10;
        for (int i = 0; i < numThreads; i++) {
            new Thread(new Incrementer(counter, numAdds, i + 1)).start();
        }
        Thread.sleep(1000);  // pauses main for 1000 msec to make sure all threads have finished before we getCount
        int expectedCount = numThreads * numAdds;
        int actualCount = counter.getCount();
        if (actualCount != expectedCount) {
            System.out.println("Thread interference!  Counter is " + actualCount + " but should be " + expectedCount + ".");
        } else {
            System.out.println("No thread interference detected. Counter is " + actualCount + ".");
        }
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
    static class Incrementer implements Runnable {

        private final Counter counter;
        private final int numIncrements;
        private final String name;

        public Incrementer(Counter counter, int numIncrements, int index) {
            this.name = "Thread " + index;
            this.counter = counter;
            this.numIncrements = numIncrements;
        }

        public void run() {
            // increment the counter numIncrements times
            for (int i = 0; i < numIncrements; i++) {
                System.out.println(name + " attempting to acquire lock.");
                getLockManager().acquireLock();
                System.out.println(name + " acquired lock.");
                counter.increment(name);
                getLockManager().releaseLock();
                System.out.println(name + " released lock.");
            }
        }
    }

    static class LockManager {
        private boolean inUse = false;

        public void acquireLock() {
            boolean waiting = true;
            while (waiting) {
                synchronized (this) {
                    // check if lock is available
                    if (!inUse) {
                        // it's not in use, so we can take it!
                        inUse = true;
                        waiting = false;
                    }
               }
               if (waiting) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) { }
               }
            }
        }

        public synchronized void releaseLock() {
            inUse = false;
        }
    }
}

