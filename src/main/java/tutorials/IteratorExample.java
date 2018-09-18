package tutorials;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This example class illustrates how one might write an iterator
 * of the most basic kind.
 * User: mhay
 * Date: 9/6/16 8:34 PM
 */
public class IteratorExample implements Iterable {

    private String[] myData;
    public IteratorExample() {
        myData = new String[]{"first", "second", "third", "fourth"};
    }

    @Override
    public Iterator<String> iterator() {
        return new MyIterator();
    }

    public static void main(String[] args) {
        System.out.println("Hi there!  Let's do some iteration!");
        IteratorExample instance = new IteratorExample();
        Iterator<String> dataIter = instance.iterator();
        while (dataIter.hasNext()) {
            String next = dataIter.next();
            System.out.println("next = " + next);
        }
        System.out.println("Our iteration has concluded.");

        // after you run it once and see how it works, uncomment the next two lines
//       System.out.println("If I call next() now, I'll get an exception");
//       dataIter.next();
    }

    /**
     * Inner class that does the iteration.  It's often a good
     * idea to make the iterator a separate class because it needs
     * to manage its own iterator-specific state (e.g., the currIdx).
     * No point in cluttering up the main class with that kind of nonsense.
     *
     * It's an inner class because (a) no one besides parent class should be
     * able to create a new iterator, and (b) as an inner class, it can access
     * the state of the parent class.
     */
    class MyIterator implements Iterator<String> {

        private int currIdx;

        public MyIterator() {
            currIdx = 0;
        }

        @Override
        public boolean hasNext() {
            return currIdx < myData.length;
        }

        @Override
        public String next() {
            if (!hasNext()) {   // always check!
                throw new NoSuchElementException();
            }
            String nextValue = myData[currIdx];
            currIdx++;
            return nextValue;
        }

        @Override
        public void remove() {
            // it's not uncommon for a class to implement the Iterator interface
            // yet not support remove.
            throw new UnsupportedOperationException("my data can't be modified!");
        }
    }
}