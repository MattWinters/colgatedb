package colgatedb.page;

import colgatedb.TestUtility;
import colgatedb.tuple.*;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static colgatedb.page.PageTestUtility.assertEqualPages;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
public class SlottedPageFormatterTest {

    private final SimplePageId pid = new SimplePageId(0, 0);

    @Test
    @GradedTest(number="7.1", max_score=1.0, visibility="visible")
    public void computePageCapacity() {
        int capacity;
        // td has one int
        capacity = SlottedPageFormatter.computePageCapacity(4, TestUtility.getTupleDesc(1));
        assertEquals(0, capacity);
        capacity = SlottedPageFormatter.computePageCapacity(8, TestUtility.getTupleDesc(1));
        assertEquals(1, capacity);
        capacity = SlottedPageFormatter.computePageCapacity(9, TestUtility.getTupleDesc(1));
        assertEquals(2, capacity);
        // td has two ints
        capacity = SlottedPageFormatter.computePageCapacity(8, TestUtility.getTupleDesc(2));
        assertEquals(0, capacity);
        capacity = SlottedPageFormatter.computePageCapacity(9, TestUtility.getTupleDesc(2));
        assertEquals(1, capacity);
        capacity = SlottedPageFormatter.computePageCapacity(17, TestUtility.getTupleDesc(2));
        assertEquals(2, capacity);

        // td has string
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.STRING_TYPE});
        capacity = SlottedPageFormatter.computePageCapacity(544, td);
        assertEquals(3, capacity);
        capacity = SlottedPageFormatter.computePageCapacity(545, td);
        assertEquals(4, capacity);
    }

    /**
     * Make sure SlottedPage now sets the number of slots correctly
     */
    @Test
    @GradedTest(number="7.2", max_score=1.0, visibility="visible")
    public void slottedPageNumSlots() {
        int pageSize = 4096;
        TupleDesc td = TestUtility.getTupleDesc(4);
        SlottedPage page = new SlottedPage(pid, td, pageSize);
        assertEquals(SlottedPageFormatter.computePageCapacity(pageSize, td), page.getNumSlots());

        pageSize = 2048;
        td = TestUtility.getTupleDesc(4);
        page = new SlottedPage(pid, td, pageSize);
        assertEquals(SlottedPageFormatter.computePageCapacity(pageSize, td), page.getNumSlots());

        pageSize = 4096;
        td = TestUtility.getTupleDesc(8);
        page = new SlottedPage(pid, td, pageSize);
        assertEquals(SlottedPageFormatter.computePageCapacity(pageSize, td), page.getNumSlots());
    }

    @Test
    @GradedTest(number="7.3", max_score=1.0, visibility="visible")
    public void testHeaderSize() {
        assertEquals(1, SlottedPageFormatter.getHeaderSize(1));
        assertEquals(1, SlottedPageFormatter.getHeaderSize(8));
        assertEquals(2, SlottedPageFormatter.getHeaderSize(9));
        assertEquals(513, SlottedPageFormatter.getHeaderSize(4097));
    }

    /**
     * Tests page single byte of header and contains tuples that have a single column of Type.INT_TYPE.
     * The test works as follows: two test objects are created
     * <p>
     * (1) a SlottedPage with data (i.e., specific tuples in specific slots)
     * <p>
     * (2) a corresponding byte representation of (1) -- this is created "by hand" according SlottedPage byte format
     * specification.
     * <p>
     * Then (2) is used to create a new SlottedPage object.  This is compared with (1) to check that they match.
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.4", max_score=1.0, visibility="visible")
    public void readFromBytes1() throws IOException {

        TestExample test1 = makeTest1(false);

        // test: create a new page from testPageBytes
        SlottedPage pageFromBytes = test1.makePage(test1.testPageBytes);

        // new page should match testPage
        assertEqualPages(test1.testPage, pageFromBytes);
    }

    /**
     * Same as readFromBytes1 except a few extra zero bytes are added to the end of the page.
     * <p>
     * I actually think it would hard to pass readFromBytes1 and fail this test because most
     * implementations will simply ignore any excess bytes when reading.  This is not true
     * for writing however, so there is a companion test for writing with excess bytes below.
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.5", max_score=1.0, visibility="visible")
    public void readFromBytes1WithExcess() throws IOException {

        TestExample test1 = makeTest1(true);

        // test: create a new page from testPageBytes
        SlottedPage pageFromBytes = test1.makePage(test1.testPageBytes);

        // new page should match testPage
        assertEqualPages(test1.testPage, pageFromBytes);
    }

    /**
     * Tests page two bytes of header and contains tuples that have a single column of Type.INT_TYPE
     * Test is similar to {@link #readFromBytes1()}
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.6", max_score=1.0, visibility="visible")
    public void readFromBytes2() throws IOException {
        TestExample test2 = makeTest2();

        // test: create a new page from testPageBytes
        SlottedPage pageFromBytes = test2.makePage(test2.testPageBytes);

        // new page should match testPage
        assertEqualPages(test2.testPage, pageFromBytes);
    }

    /**
     * Tests page one byte of header and contains tuples that have two columns of Type.INT_TYPE
     * Test is similar to {@link #readFromBytes1()}
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.7", max_score=1.0, visibility="visible")
    public void readFromBytes3() throws IOException {
        TestExample test3 = makeTest3();

        // test: create a new page from testPageBytes
        SlottedPage pageFromBytes = test3.makePage(test3.testPageBytes);

        // new page should match testPage
        assertEqualPages(test3.testPage, pageFromBytes);
    }

    /**
     * Checks that the beforeImage is set *after* the page is read from bytes.
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.8", max_score=1.0, visibility="visible")
    public void setBeforeImage() throws IOException {
        TestExample test1 = makeTest1(false);

        // test: create a new page from testPageBytes
        SlottedPage pageFromBytes = test1.makePage(test1.testPageBytes);

        // before image of new page should match testPage
        assertEqualPages(test1.testPage, (SlottedPage) pageFromBytes.getBeforeImage());
    }

    /**
     * Test that bytes are correctly written.
     *
     * Set up for the test is the same as {@link #readFromBytes1()} except that now we are taking the SlottedPage
     * testPage and calling {@link SlottedPage#getPageData()} on it to obtain bytes and comparing to the expected
     * bytes {@link TestExample#testPageBytes}.
     *
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.9", max_score=1.0, visibility="visible")
    public void writeToBytes1() throws IOException {
        TestExample test1 = makeTest1(false);
        byte[] pageBytes = test1.testPage.getPageData();
        assertArrayEquals(test1.testPageBytes, pageBytes);
    }

    /**
     * Same as writeToBytes1 except a few extra zero bytes are added to the end of the page.
     * @throws IOException
     */
    @Test
    @GradedTest(number="7.10", max_score=1.0, visibility="visible")
    public void writeToBytes1WithExcess() throws IOException {
        TestExample test1 = makeTest1(true);
        byte[] pageBytes = test1.testPage.getPageData();
        assertArrayEquals(test1.testPageBytes, pageBytes);
    }

    @Test
    @GradedTest(number="7.11", max_score=1.0, visibility="visible")
    public void writeToBytes2() throws IOException {
        TestExample test2 = makeTest2();
        byte[] pageBytes = test2.testPage.getPageData();
        assertArrayEquals(test2.testPageBytes, pageBytes);
    }

    @Test
    @GradedTest(number="7.12", max_score=1.0, visibility="visible")
    public void writeToBytes3() throws IOException {
        TestExample test3 = makeTest3();
        byte[] pageBytes = test3.testPage.getPageData();
        assertArrayEquals(test3.testPageBytes, pageBytes);
    }

    /**
     * Creates a page with tuples having both ints and strings and tests that an equivalent page
     * can be recreated from its byte data.
     */
    @Test
    @GradedTest(number="7.13", max_score=1.0, visibility="visible")
    public void toBytesAndBackStringTypes() {
        TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.STRING_TYPE}, new String[]{"id", "name"});
        int pageSize = 512;
        SlottedPage testPage = new SlottedPage(pid, td, pageSize);
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(42));
        tuple.setField(1, new StringField("bob"));
        testPage.insertTuple(tuple);
        byte[] pageData = testPage.getPageData();

        SlottedPage pageFromBytes = new SlottedPage(pid, td, pageSize, pageData);

        assertEqualPages(testPage, pageFromBytes);
    }

    /**
     * Generate lots of pages with different schemas.  For each page, write it out to bytes, read it in and check that
     * it matches the original.
     */
    @Test
    @GradedTest(number="7.14", max_score=1.0, visibility="visible")
    public void readWriteTest() {
        int[] pageSizes = new int[]{1024, 2048, 4096};
        int[] schemaSizes = new int[]{1, 10, 20};
        for (int pageSize : pageSizes) {
            for (int schemaSize : schemaSizes) {
                TupleDesc td = TestUtility.getTupleDesc(schemaSize);
                SlottedPage page = new SlottedPage(pid, td, pageSize);
                for (int i = 0; i < page.getNumSlots(); i++) {
                    // insert some tuples here and there; not much rhyme or reason to this particular condition
                    // except that there will be stretches of empty slots
                    if (i % 7 <= 1 || i % 5 == 0) {
                        page.insertTuple(TestUtility.getIntTuple(i, td.numFields()));
                    }
                }
                byte[] data = page.getPageData();
                SlottedPage pageFromBytes = new SlottedPage(pid, td, pageSize, data);
                // if this doesn't pass, you might add print statements above to identify on which time thru the loop
                // it fails...
                assertEqualPages(page, pageFromBytes);

                byte[] data2 = pageFromBytes.getPageData();  // this is starting to get a little silly...
                assertArrayEquals(data, data2);
            }
        }
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }


    /**
     * Creates a 17 byte page that can hold 4 tuples, each containing a single INT, plus one byte for the header.
     * Records are put in slots 0 and 3.
     * @param addExcessBytes whether to make the page a little bigger (3 extra bytes are added)
     * @return
     * @throws IOException
     */
    private TestExample makeTest1(boolean addExcessBytes) throws IOException {
        // n = 1 columns (of type INT)
        // b = 4*n*8 + 1 bits per tuple
        // k = number of slots
        // pageSize = ceiling( (k * b) / 8 )
        // header = ceiling(k / 8)

        // k = 4
        // header = 1 byte
        // pageSize = 17
        int excessBytes = 0;
        if (addExcessBytes) {
            excessBytes = 3;
        }
        int pageSize = 17 + excessBytes;   // 1 byte for header + 16 bytes for 4 tuples, each 4 bytes
        TupleDesc td = TestUtility.getTupleDesc(1);

        // make testPage (a SlottedPage object) with data in slots 0 and 3
        SlottedPage testPage = new SlottedPage(pid, td, pageSize);
        Tuple t1 = TestUtility.getIntTuple(new int[]{15});
        Tuple t2 = TestUtility.getIntTuple(new int[]{33});
        testPage.insertTuple(0, t1);
        testPage.insertTuple(3, t2);


        // make testPageBytes (the corresponding byte array)
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
        DataOutputStream dos = new DataOutputStream(baos);
        // thus, the header should be 00001001 in binary (slots 0 and 3 occupied), or 09 in Hexadecimal
        // this looks "backwards" b/c we're writing out the bits w/in the byte from least to most significant
        // (The low (least significant) bits of each byte represents the status of the slots that are earlier in the
        // page. Hence, the lowest bit of the first byte represents whether or not the first slot in the page is in use.)
        byte[] header = hexStringToByteArray("09");
        dos.write(header);
        // now we write out the tuples
        dos.writeInt(15); // t1
        dos.writeInt(0);  // empty slot
        dos.writeInt(0);  // empty slot
        dos.writeInt(33); // t2
        if (addExcessBytes) {
            dos.write(new byte[excessBytes]);
        }
        dos.flush();
        byte[] testPageBytes = baos.toByteArray();

        return new TestExample(td, pageSize, testPage, testPageBytes);
    }

    /**
     * Creates a 38 byte page that can hold 9 tuples, each containing a single INT, plus two bytes for the header.
     * Records are put in slots 2 and 7, and 8.
     * @return
     * @throws IOException
     */
    private TestExample makeTest2() throws IOException {
        // n = 1 columns (of type INT)
        // b = 4*n*8 + 1 bits per tuple
        // k = number of slots
        // pageSize = ceiling( (k * b) / 8 )
        // header = ceiling(k / 8)

        // k = 9
        // header = 2 bytes
        // pageSize = 38
        int pageSize = 38;  // 2 bytes for header + 36 bytes for 9 tuples, each 4 bytes
        TupleDesc td = TestUtility.getTupleDesc(1);

        // make testPage (a SlottedPage object)
        SlottedPage testPage = new SlottedPage(pid, td, pageSize);
        Tuple t1 = TestUtility.getIntTuple(new int[]{1000000000});
        Tuple t2 = TestUtility.getIntTuple(new int[]{-12});
        Tuple t3 = TestUtility.getIntTuple(new int[]{43});
        testPage.insertTuple(2, t1);
        testPage.insertTuple(7, t2);
        testPage.insertTuple(8, t3);


        // make testPageBytes (the corresponding byte array)
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
        DataOutputStream dos = new DataOutputStream(baos);
        // thus, the header should be 10000100  00000001 in binary, or 8401 in Hexadecimal
        //                slot nos => 7    2           8
        byte[] header = hexStringToByteArray("8401");
        dos.write(header);
        // now we write out the tuples
        dos.writeInt(0);
        dos.writeInt(0); // empty slots
        dos.writeInt(1000000000);  // t1
        dos.writeInt(0);
        dos.writeInt(0);
        dos.writeInt(0);
        dos.writeInt(0); // empty slots
        dos.writeInt(-12); // t2
        dos.writeInt(43);  // t3
        dos.flush();
        byte[] testPageBytes = baos.toByteArray();

        return new TestExample(td, pageSize, testPage, testPageBytes);
    }

    /**
     * Creates a 65 byte page that can hold 8 tuples, each containing two INTs, plus one byte for the header.
     * Records are put in slots 0, 1, and 6.
     * @return
     * @throws IOException
     */
    private TestExample makeTest3() throws IOException {
        // n = 2 columns (of type INT)
        // b = 4*n*8 + 1 bits per tuple
        // k = number of slots
        // pageSize = ceiling( (k * b) / 8 )
        // header = ceiling(k / 8)

        // k = 8
        // header = 1 byte
        // pageSize = 65
        int pageSize = 65;  // 1 bytes for header + 8 bytes for 8 tuples, each 2 bytes
        TupleDesc td = TestUtility.getTupleDesc(2);

        // make testPage (a SlottedPage object)
        SlottedPage testPage = new SlottedPage(pid, td, pageSize);
        Tuple t1 = TestUtility.getIntTuple(new int[]{1, 10});
        Tuple t2 = TestUtility.getIntTuple(new int[]{-1, -2});
        Tuple t3 = TestUtility.getIntTuple(new int[]{42, -42});
        testPage.insertTuple(0, t1);
        testPage.insertTuple(1, t2);
        testPage.insertTuple(6, t3);


        // make testPageBytes (the corresponding byte array)
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
        DataOutputStream dos = new DataOutputStream(baos);
        // thus, the header should be 01000011 in binary, or 43 in Hexadecimal
        byte[] header = hexStringToByteArray("43");
        dos.write(header);
        // now we write out the tuples
        dos.writeInt(1);
        dos.writeInt(10);  // t1 (2 ints wide)
        dos.writeInt(-1);
        dos.writeInt(-2); // t2
        dos.writeInt(0);
        dos.writeInt(0); // empty slot (2 ints wide)
        dos.writeInt(0);
        dos.writeInt(0); // empty slot
        dos.writeInt(0);
        dos.writeInt(0); // empty slot
        dos.writeInt(0);
        dos.writeInt(0); // empty slot
        dos.writeInt(42);
        dos.writeInt(-42); // t3
        dos.writeInt(0);
        dos.writeInt(0); // empty slot
        dos.flush();
        byte[] testPageBytes = baos.toByteArray();

        return new TestExample(td, pageSize, testPage, testPageBytes);
    }

    /**
     * Class to encapsulate a test example
     */
    private class TestExample {
        private final TupleDesc td;
        private final int pageSize;
        SlottedPage testPage;
        byte[] testPageBytes;

        public TestExample(TupleDesc td, int pageSize, SlottedPage testPage, byte[] testPageBytes) {
            this.td = td;
            this.pageSize = pageSize;
            this.testPage = testPage;
            this.testPageBytes = testPageBytes;
        }

        public SlottedPage makePage(byte [] bytes) {
            return new SlottedPage(pid, td, pageSize, bytes);
        }
    }
}
