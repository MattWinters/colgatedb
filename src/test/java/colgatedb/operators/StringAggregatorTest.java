package colgatedb.operators;

import colgatedb.tuple.Type;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

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
public class StringAggregatorTest {

  int width1 = 2;
  DbIterator scan1;
  int[][] count = null;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleList() throws Exception {
    this.scan1 = OperatorTestUtility.createTupleList(width1,
            new Object[]{1, "a",
                    1, "b",
                    1, "c",
                    3, "d",
                    3, "e",
                    3, "f",
                    5, "g"});

    // verify how the results progress after a few merges
    this.count = new int[][] {
      { 1, 1 },
      { 1, 2 },
      { 1, 3 },
      { 1, 3, 3, 1 }
    };

  }

  /**
   * Test String.mergeTupleIntoGroup() and iterator() over a COUNT
   */
  @Test public void mergeCount() throws Exception {
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);

    for (int[] step : count) {
      agg.mergeTupleIntoGroup(scan1.next());
      DbIterator it = agg.iterator();
      it.open();
      OperatorTestUtility.matchAllTuples(OperatorTestUtility.createTupleList(width1, step), it);
    }
  }


  /**
   * Test StringAggregator.iterator() for DbIterator behaviour
   */
  @Test public void testIterator() throws Exception {
    // first, populate the aggregator via sum over scan1
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);
    try {
      while (true)
        agg.mergeTupleIntoGroup(scan1.next());
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }

    DbIterator it = agg.iterator();
    it.open();

    // verify it has three elements
    int count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // rewind and try again
    it.rewind();
    count = 0;
    try {
      while (true) {
        it.next();
        count++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, count);

    // close it and check that we don't get anything
    it.close();
    try {
      it.next();
      throw new Exception("StringAggreator iterator yielded tuple after close");
    } catch (Exception e) {
      // explicitly ignored
    }
  }

}

