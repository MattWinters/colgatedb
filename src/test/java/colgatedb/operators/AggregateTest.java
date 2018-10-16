package colgatedb.operators;

import colgatedb.TestUtility;
import colgatedb.tuple.TupleDesc;
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
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */
public class AggregateTest {

  int width1 = 2;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator scan3;

  DbIterator sum;
  DbIterator sumstring;

  DbIterator avg;
  DbIterator max;
  DbIterator min;
  DbIterator count;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {	  
    this.scan1 = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7});
    this.scan2 = OperatorTestUtility.createTupleList(width1,
            new Object[]{1, "a",
                    1, "a",
                    1, "a",
                    3, "a",
                    3, "a",
                    3, "a",
                    5, "a"});
    this.scan3 = OperatorTestUtility.createTupleList(width1,
            new Object[]{"a", 2,
                    "a", 4,
                    "a", 6,
                    "b", 2,
                    "b", 4,
                    "b", 6,
                    "c", 7});

    this.sum = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 12,
                    3, 12,
                    5, 7});
    this.sumstring = OperatorTestUtility.createTupleList(width1,
            new Object[]{"a", 12,
                    "b", 12,
                    "c", 7});

    this.avg = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 4,
                    3, 4,
                    5, 7});
    this.min = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 2,
                    3, 2,
                    5, 7});
    this.max = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 6,
                    3, 6,
                    5, 7});
    this.count = OperatorTestUtility.createTupleList(width1,
            new int[]{1, 3,
                    3, 3,
                    5, 1});

  }


  /**
   * Unit test for Aggregate.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    Aggregate op = new Aggregate(scan1, 0, 0,
        Aggregator.Op.MIN);
    TupleDesc expected = TestUtility.getTupleDesc(2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Aggregate.rewind()
   */
  @Test public void rewind() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.MIN);
    op.open();
    while (op.hasNext()) {
      assertNotNull(op.next());
    }
    assertTrue(OperatorTestUtility.checkExhausted(op));

    op.rewind();
    min.open();
    OperatorTestUtility.matchAllTuples(min, op);
  }


  /**
   * Unit test for Aggregate.getNext() using a count aggregate with string types
   */
  @Test public void countStringAggregate() throws Exception {
    Aggregate op = new Aggregate(scan2, 1, 0,
        Aggregator.Op.COUNT);
    op.open();
    count.open();
    OperatorTestUtility.matchAllTuples(count, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a count aggregate with string types
   */
  @Test public void sumStringGroupBy() throws Exception {
    Aggregate op = new Aggregate(scan3, 1, 0,
        Aggregator.Op.SUM);
    op.open();
    sumstring.open();
    OperatorTestUtility.matchAllTuples(sumstring, op);
  }


  /**
   * Unit test for Aggregate.getNext() using a sum aggregate
   */
  @Test public void sumAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.SUM);
    op.open();
    sum.open();
    OperatorTestUtility.matchAllTuples(sum, op);
  }

  /**
   * Unit test for Aggregate.getNext() using an avg aggregate
   */
  @Test public void avgAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
       Aggregator.Op.AVG);
    op.open();
    avg.open();
    OperatorTestUtility.matchAllTuples(avg, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a max aggregate
   */
  @Test public void maxAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
        Aggregator.Op.MAX);
    op.open();
    max.open();
    OperatorTestUtility.matchAllTuples(max, op);
  }

  /**
   * Unit test for Aggregate.getNext() using a min aggregate
   */
  @Test public void minAggregate() throws Exception {
    Aggregate op = new Aggregate(scan1, 1, 0,
       Aggregator.Op.MIN);
    op.open();
    min.open();
    OperatorTestUtility.matchAllTuples(min, op);
  }

}

