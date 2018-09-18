package colgatedb.tuple;

import colgatedb.TestUtility;
import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
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
public class RecordIdTest {

    private static RecordId rid;
    private static RecordId rid2;
    private static RecordId rid3;
    private static RecordId rid4;


    @Before
    public void createPids() {
        PageId pid = new SimplePageId(-1, 2);
        PageId pid2 = new SimplePageId(-1, 2);
        PageId pid3 = new SimplePageId(-2, 2);
        rid = new RecordId(pid, 3);
        rid2 = new RecordId(pid2, 3);
        rid3 = new RecordId(pid, 4);
        rid4 = new RecordId(pid3, 3);

    }

    @Test
    public void getPageId() {
        PageId pid = new SimplePageId(-1, 2);
        assertEquals(pid, rid.getPageId());

    }

    /**
     * Unit test for RecordId.tupleno()
     */
    @Test
    public void tupleno() {
        assertEquals(3, rid.tupleno());
    }

    /**
     * Unit test for RecordId.equals()
     */
    @Test
    public void equals() {
        assertEquals(rid, rid2);
        assertEquals(rid2, rid);
        assertNotEquals(rid, rid3);
        assertNotEquals(rid3, rid);
        assertNotEquals(rid2, rid4);
        assertNotEquals(rid4, rid2);
    }

    @Test
    public void hCode() {
        assertEquals(rid.hashCode(), rid2.hashCode());
    }

    @Test
    public void tupleMethods() {
        Tuple t = TestUtility.getIntTuple(5);
        assertNull(t.getRecordId());
        t.setRecordId(rid);
        assertEquals(rid, t.getRecordId());
    }

}

