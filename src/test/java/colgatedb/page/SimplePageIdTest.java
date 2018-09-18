package colgatedb.page;

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
public class SimplePageIdTest {

    private SimplePageId pid;

    @Before
    public void createPid() {
        pid = new SimplePageId(1, 2);
    }


    @Test
    public void getTableId() {
        assertEquals(1, pid.getTableId());
    }


    @Test
    public void pageno() {
        assertEquals(2, pid.pageNumber());
    }


    @Test
    public void testHashCode() {
        int code1, code2;

        // the hashCode could be anything. test determinism, at least.
        code1 = pid.hashCode();
        assertEquals(code1, pid.hashCode());
        assertEquals(code1, pid.hashCode());

        pid = new SimplePageId(2, 2);
        code2 = pid.hashCode();
        assertEquals(code2, pid.hashCode());
        assertEquals(code2, pid.hashCode());
    }

    @Test
    public void testBadHashCode() {
        int code1, code2;

        code1 = new SimplePageId(12, 0).hashCode();
        code2 = new SimplePageId(1, 20).hashCode();
        // while the hashCode could be anything, these test catches particularly bad
        // hashcode implementations
        assertNotSame(code1, code2);
    }

    @Test
    public void equals() {
        SimplePageId pid1 = new SimplePageId(1, 1);
        SimplePageId pid1Copy = new SimplePageId(1, 1);
        SimplePageId pid2 = new SimplePageId(2, 2);

        // .equals() with null should return false
        assertFalse(pid1.equals(null));

        // .equals() with the wrong type should return false
        assertFalse(pid1.equals(new Object()));

        assertTrue(pid1.equals(pid1));
        assertTrue(pid1.equals(pid1Copy));
        assertTrue(pid1Copy.equals(pid1));
        assertTrue(pid2.equals(pid2));

        assertFalse(pid1.equals(pid2));
        assertFalse(pid1Copy.equals(pid2));
        assertFalse(pid2.equals(pid1));
        assertFalse(pid2.equals(pid1Copy));
    }

    @Test
    public void testToString() {
        SimplePageId pid = new SimplePageId(10, 99);
        assertEquals("10-99", pid.toString());
    }

}

