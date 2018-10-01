package colgatedb;


import colgatedb.page.*;
import org.junit.Before;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class BufferManagerTest {
    private MockDiskManager dm;
    private PageMaker pm;
    private BufferManager buffMgr;
    private int tableid = 0;
    private PageId pid0 = new SimplePageId(tableid, 0);
    private PageId pid1 = new SimplePageId(tableid, 1);
    private PageId pid2 = new SimplePageId(tableid, 2);
    private PageId pid3 = new SimplePageId(tableid, 3);  // this one is not allocated initially

    @Before
    public void setUp() throws IOException {
        dm = new MockDiskManager();
        pm = dm;
        for (int i = 0; i < 3; i++) {
            SimplePageId pid = new SimplePageId(tableid, i);
            dm.allocatePage(pid);
            dm.setDatum(pid, pid.pageNumber());
        }
    }

    private void initializeBufferManager(int numPages) {
        buffMgr = new BufferManagerImpl(numPages, dm);
        buffMgr.evictDirty(true);
    }

    /**
     * Test that pinPage returns correct page.
     */
    @Test
    public void testPinPage() {
        initializeBufferManager(2);
        Page page = buffMgr.pinPage(pid0, pm);
        // check that it equals the desired page
        assertEquals(page, dm.getPage(pid0));

        // read a different page
        page = buffMgr.pinPage(pid1, pm);
        // check it was read once from MockDiskManager
        assertEquals(page, dm.getPage(pid1));

        // check each page was read once from MockDiskManager
        assertEquals(1, dm.getReadCount(pid0));
        assertEquals(1, dm.getReadCount(pid1));
    }

    /**
     * Test that page is read from disk on first pin and
     * read from cache on second pin.
     */
    @Test
    public void testPinPageTwice() {
        initializeBufferManager(1);
        buffMgr.pinPage(pid0, pm);
        assertEquals(1, dm.getReadCount(pid0));

        // pinning a second time: page should be in cache and not be read from disk again
        buffMgr.pinPage(pid0, pm);
        assertEquals(1, dm.getReadCount(pid0));
    }

    /**
     * Tests that flushPage only flushes pages that are marked as dirty
     */
    @Test
    public void flushIfDirty() {
        initializeBufferManager(1);
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);
        buffMgr.flushPage(pid0);
        assertEquals(10, dm.getDatum(pid0));
    }

    /**
     * Tests that flushPage only flushes pages that are marked as dirty
     */
    @Test
    public void flushOnlyDirty() {
        initializeBufferManager(1);
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, false);  // marking as clean when it's really dirty
        buffMgr.flushPage(pid0);
        assertEquals(0, dm.getDatum(pid0));
    }

    @Test
    public void isDirty() {
        initializeBufferManager(2);
        buffMgr.pinPage(pid0, pm);
        buffMgr.unpinPage(pid0, true);  // marking this page as dirty even though it's not

        buffMgr.pinPage(pid1, pm);
        buffMgr.unpinPage(pid1, false);  // marking this page as dirty even though it's not

        assertTrue(buffMgr.isDirty(pid0));
        assertFalse(buffMgr.isDirty(pid1));
        assertFalse(buffMgr.isDirty(pid2));
    }


    /**
     * Tests that flushPage only flushes pages that are marked as dirty
     */
    @Test
    public void flushAllPages() {
        initializeBufferManager(3);
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);

        page = (MockPage) buffMgr.pinPage(pid1, pm);
        page.datum = 11;
        buffMgr.unpinPage(pid1, true);

        page = (MockPage) buffMgr.pinPage(pid2, pm);
        page.datum = 12;
        buffMgr.unpinPage(pid2, false);

        buffMgr.flushAllPages();

        assertEquals(10, dm.getDatum(pid0));
        assertEquals(11, dm.getDatum(pid1));
        assertEquals(2, dm.getDatum(pid2));   // page was not marked as dirty
    }

    /**
     * Tests that dirty bit stays dirty
     */
    @Test
    public void dirtyBitStaysDirty() {
        initializeBufferManager(1);

        // first user pins page and dirties it
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);

        // second user pins the page but doesn't modify it, but page should still be considered dirty
        // because it has modifications that have not been flushed to disk yet!
        buffMgr.pinPage(pid0, pm);
        buffMgr.unpinPage(pid0, false);

        buffMgr.flushPage(pid0);
        assertEquals(10, dm.getDatum(pid0));
    }

    /**
     * Tests that unpinning doesn't flush prematurely.
     */
    @Test
    public void unPinShouldNotFlush() {
        initializeBufferManager(1);
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);

        // page never flushed and should not have been evicted: value on disk should be 0
        assertEquals(0, dm.getDatum(pid0));
    }

    /**
     * Simple test that eviction happens
     */
    @Test
    public void evictPage() {
        initializeBufferManager(1);
        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);

        // value on disk shouldn't change yet
        assertEquals(0, dm.getDatum(pid0));

        // pin a second page, should trigger eviction of first
        buffMgr.pinPage(pid1, pm);

        // if page was evicted, then value on disk would change
        assertEquals(10, dm.getDatum(pid0));
    }

    @Test
    public void doNotEvictPinned() {
        initializeBufferManager(1);
        buffMgr.pinPage(pid0, pm);
        try {
            // try pin a second page, but since pool has only 1 frame and it's still pinned, there's no room!
            buffMgr.pinPage(pid1, pm);
            fail("Should have raised exception!");
        } catch (BufferManagerException e) {
            // expected
        }
    }

    /**
     * This tests that pin counts are actually being maintained b/c a page is
     * pinned twice and unpinned only once so it's still not a candidate for eviction.
     */
    @Test
    public void doNotEvictPinned2() {
        initializeBufferManager(1);
        buffMgr.pinPage(pid0, pm);
        buffMgr.pinPage(pid0, pm);    // pin it twice
        buffMgr.unpinPage(pid0, false);
        try {
            // try pin a second page, but since pool has only 1 frame and it's still pinned, there's no room!
            buffMgr.pinPage(pid1, pm);
            fail("Should have raised exception!");
        } catch (BufferManagerException e) {
            // expected
        }
    }

    @Test
    public void allowEvictDirty() {
        runAllowEvictDirtyTest(true);
        runAllowEvictDirtyTest(false);
    }

    public void runAllowEvictDirtyTest(boolean allowEvictDirty) {
        initializeBufferManager(1);
        buffMgr.evictDirty(allowEvictDirty);
        buffMgr.pinPage(pid0, pm);
        buffMgr.unpinPage(pid0, false);

        // since pid0 is not dirty, you should be able to evict it
        buffMgr.pinPage(pid1, pm);
        buffMgr.unpinPage(pid1, true);

        try {
            // try pin a third page
            // buffer pool has only 1 frame and it contains a dirty page
            buffMgr.pinPage(pid2, pm);
            // if allowEvictDirty is set to false, there's no room!
            if (!allowEvictDirty) {
                fail("Should have raised exception!");
            }
        } catch (BufferManagerException e) {

            // on the other hand, if allowEvictDirty is set to true, then
            // you should have been able to evict pid1 to make room for pid2
            if (allowEvictDirty) {
                fail("Should NOT have raised exception!");
            }
        }
    }


    @Test
    public void unpinPageThatIsNotThere() {
        initializeBufferManager(1);
        try {
            buffMgr.unpinPage(pid0, false);
            fail("Should have raised exception!");
        } catch (BufferManagerException e) {
            // expected
        }
    }

    @Test
    public void unpinPageTwice() {
        initializeBufferManager(1);
        buffMgr.pinPage(pid0, pm);
        buffMgr.unpinPage(pid0, false);
        try {
            // pin count already zero, this should throw an exception
            buffMgr.unpinPage(pid0, false);
            fail("Should have raised exception!");
        } catch (BufferManagerException e) {
            // expected
        }
    }

    /**
     * Tests that buffer manager calls allocatePage on disk manager.
     */
    @Test
    public void allocatePage() {
        initializeBufferManager(1);
        buffMgr.allocatePage(pid3);
        assertEquals(-1, dm.getDatum(pid3));
    }

    @Test
    public void inBufferPool() {
        initializeBufferManager(2);
        buffMgr.pinPage(pid0, pm);
        buffMgr.pinPage(pid2, pm);

        assertTrue(buffMgr.inBufferPool(pid0));
        assertFalse(buffMgr.inBufferPool(pid1));
        assertTrue(buffMgr.inBufferPool(pid2));
    }

    @Test
    public void getPage() {
        initializeBufferManager(2);
        Page page = buffMgr.pinPage(pid0, pm);
        Page page2 = buffMgr.pinPage(pid2, pm);

        assertEquals(page, buffMgr.getPage(pid0));
        assertEquals(page2, buffMgr.getPage(pid2));

        try {
            buffMgr.getPage(pid1);
            fail("Page not in buffer pool.  Should raise exception!");
        } catch (BufferManagerException e) {
            // expected
        }
    }

    @Test
    public void testDiscardPage() {
        initializeBufferManager(2);

        MockPage page = (MockPage) buffMgr.pinPage(pid0, pm);
        page.datum = 10;
        buffMgr.unpinPage(pid0, true);
        buffMgr.discardPage(pid0);
        assertEquals(1, dm.getReadCount(pid0));
        assertEquals(0, dm.getWriteCount(pid0));
        assertEquals(0, dm.getDatum(pid0));

        page = (MockPage) buffMgr.pinPage(pid0, pm);
        assertEquals(0, page.datum);
        assertEquals(2, dm.getReadCount(pid0));

        buffMgr.discardPage(pid1);  // discarding a page that's not there
        assertEquals(0, dm.getReadCount(pid1));
    }

    @Test
    public void testManyReadsWithEviction() {
        initializeBufferManager(1);
        int numRounds = 1;
        PageId[] pids = new PageId[]{pid0, pid1, pid2};

        for (int round = 0; round < numRounds; round++) {
            for (PageId pid : pids) {
                MockPage page = (MockPage) buffMgr.pinPage(pid, pm);
                page.datum++;
                buffMgr.unpinPage(pid, true);
            }
        }
        for (PageId pid : pids) {
            assertEquals(numRounds, dm.getReadCount(pid));  // buffer pool is small, each pin requires an eviction
        }
        buffMgr.flushPage(pid2); // last page is still in buffer pool
        for (PageId pid : pids) {
            assertEquals(numRounds + pid.pageNumber(), dm.getDatum(pid));
        }
    }

    @Test
    public void testManyReadsBigPool() {
        initializeBufferManager(3);
        int numRounds = 100;
        PageId[] pids = new PageId[]{pid0, pid1, pid2};

        for (int round = 0; round < numRounds; round++) {
            for (PageId pid : pids) {
                MockPage page = (MockPage) buffMgr.pinPage(pid, pm);
                page.datum++;
                buffMgr.unpinPage(pid, true);
            }
        }
        for (PageId pid : pids) {
            assertEquals(1, dm.getReadCount(pid));  // buffer is big enough to hold all pages, so each should only be read once
            assertEquals(pid.pageNumber(), dm.getDatum(pid));
        }
        buffMgr.flushAllPages();
        for (PageId pid : pids) {
            assertEquals(numRounds + pid.pageNumber(), dm.getDatum(pid));
        }

    }

    /**
     * MockDiskManager is a fake disk manager used for testing purposes.
     *
     * Testers can inspect the disk manager to see how many times a
     * particular page was read or written.
     *
     */
    public class MockDiskManager implements DiskManager, PageMaker {

        // keep track of reads and writes (and allocations?)
        List<PageContainer> pages = new ArrayList<>();

        @Override
        public void allocatePage(PageId pid) {
            assertEquals(tableid, pid.getTableId());
            assertEquals(pages.size(), pid.pageNumber());
            pages.add(new PageContainer());
        }

        @Override
        public Page readPage(PageId pid, PageMaker pageMaker) {
            assertEquals(this, pageMaker);
            PageContainer container = getPageContainer(pid);
            container.reads++;
            return new MockPage(pid, container.pageDatum);
        }

        @Override
        public void writePage(Page page) {
            PageId pid = page.getId();
            PageContainer container = getPageContainer(pid);
            container.writes++;
            container.pageDatum = ((MockPage)page).datum;
        }

        public PageContainer getPageContainer(PageId pid) {
            assertTrue(0 <= pid.pageNumber() && pid.pageNumber() < pages.size());
            return pages.get(pid.pageNumber());
        }

        /**
         * @return number of times page with pid was read
         */
        public int getReadCount(PageId pid) {
            return getPageContainer(pid).reads;
        }

        /**
         * @return number of times page with pid was written
         */
        public int getWriteCount(PageId pid) {
            return getPageContainer(pid).writes;
        }

        /**
         * @return number of times page with pid was written
         */
        public int getDatum(PageId pid) {
            return getPageContainer(pid).pageDatum;
        }

        /**
         * This disk manager does not use the PageMaker because it reads/writes MockPages.
         * @param pid
         * @param bytes
         * @return
         */
        @Override
        public Page makePage(PageId pid, byte[] bytes) {
            throw new UnsupportedOperationException();
        }

        /**
         * This disk manager does not use the PageMaker because it reads/writes MockPages.
         * @param pid
         * @return
         */
        @Override
        public Page makePage(PageId pid) {
            throw new UnsupportedOperationException();
        }

        public MockPage getPage(PageId pid) {
            return new MockPage(pid, getPageContainer(pid).pageDatum);
        }

        public void setPage(Page page) {
            PageId pid = page.getId();
            PageContainer container = getPageContainer(pid);
            container.pageDatum = ((MockPage)page).datum;
        }

        public void setDatum(PageId pid, int datum) {
            PageContainer container = getPageContainer(pid);
            container.pageDatum = datum;
        }

        private class PageContainer {
            int pageDatum;
            int reads;
            int writes;
            public PageContainer() {
                this.pageDatum = -1;
                reads = 0;
                writes = 0;
            }
        }
    }

    public class MockPage implements Page {

        private final PageId pid;
        int datum;

        public MockPage(PageId pid, int datum) {
            this.pid = pid;
            this.datum = datum;
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof MockPage) &&
                    ((((MockPage)other).datum == datum) &&
                            ((MockPage)other).getId().equals(this.getId()));
        }

        @Override
        public PageId getId() {
            return pid;
        }

        @Override
        public byte[] getPageData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getBeforeImage() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBeforeImage() {
            throw new UnsupportedOperationException();
        }
    }
}
