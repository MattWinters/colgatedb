package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.page.SimplePageId;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
public class AccessManagerTestBase {
    protected TransactionId tid0 = new TransactionId();
    protected TransactionId tid1 = new TransactionId();
    protected TransactionId tid2 = new TransactionId();
    protected SimplePageId pid0 = new SimplePageId(0, 0);
    protected SimplePageId pid1 = new SimplePageId(0, 1);

    protected MockBufferManager bm;
    protected AccessManagerImpl am;
    protected MockPageMaker pm = new MockPageMaker();

    @Before
    public void setUp() throws IOException {
        bm = new MockBufferManager();
        am = new AccessManagerImpl(bm);
    }

    /**
     * Helper class.  Give AccessManager a fake buffer manager so that it's easy
     * to track whether certain methods were called.
     */
    public class MockBufferManager implements BufferManager {
        Map<PageId, Integer> pinCount = new HashMap<>();
        Set<PageId> pageAllocations = new HashSet<>();
        Set<PageId> flushedPages = new HashSet<>();
        Set<PageId> dirtyPages = new HashSet<>();
        Map<PageId, Page> bufferPool = new HashMap<>();
        Page[] pages = new Page[]{new MockPage(pid0, 10), new MockPage(pid1, 11)};

        public MockBufferManager() {
            pinCount.putIfAbsent(pid0, 0);
            pinCount.putIfAbsent(pid1, 0);
        }

        @Override
        public Page pinPage(PageId pid, PageMaker pageMaker) {
            pinCount.put(pid, pinCount.get(pid) + 1);
            Page page = pages[pid.pageNumber()];
            bufferPool.put(pid, page);
            return page;
        }

        @Override
        public void unpinPage(PageId pid, boolean isDirty) {
            pinCount.put(pid, pinCount.get(pid) - 1);
            if (isDirty) {
                dirtyPages.add(pid);
            }
        }

        @Override
        public void flushPage(PageId pid) {
            if (dirtyPages.contains(pid)) {
                flushedPages.add(pid);
            }
            dirtyPages.remove(pid);
        }

        @Override
        public void flushAllPages() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evictDirty(boolean allowEvictDirty) {}

        @Override
        public void allocatePage(PageId pid) {
            pageAllocations.add(pid);
        }

        @Override
        public boolean isDirty(PageId pid) {
            return dirtyPages.contains(pid);
        }

        @Override
        public boolean inBufferPool(PageId pid) {
            return bufferPool.containsKey(pid);
        }

        @Override
        public Page getPage(PageId pid) {
            return bufferPool.get(pid);
        }

        @Override
        public void discardPage(PageId pid) {
            bufferPool.remove(pid);
        }

        public int pinCount(PageId pid) {
            return pinCount.get(pid);
        }

        public boolean isPageAllocated(PageId pid) {
            return pageAllocations.contains(pid);
        }

        public boolean wasFlushed(PageId pid) {
            return flushedPages.contains(pid);
        }
    }

    /**
     * Simple Page object that just stores a single (byte-sized) integer
     */
    public static class MockPage implements Page {

        private final PageId pid;
        byte datum;
        byte beforeImage;
        public final static int PAGESIZE = 1;  // 1 byte

        public MockPage(PageId pid, int datum) {
            this(pid, new byte[]{(byte) datum});
        }

        public MockPage(PageId pid, byte[] pageData) {
            this.pid = pid;
            if (pageData.length != 1) {
                throw new RuntimeException("Invalid input!");
            }
            this.datum = pageData[0];
            beforeImage = datum;
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof MockPage) &&
                    ((((MockPage) other).datum == datum) &&
                            ((MockPage) other).getId().equals(this.getId()));
        }

        @Override
        public PageId getId() {
            return pid;
        }

        @Override
        public byte[] getPageData() {
            return new byte[]{datum};
        }

        @Override
        public Page getBeforeImage() {
            return new MockPage(pid, beforeImage);
        }

        @Override
        public void setBeforeImage() {
            beforeImage = datum;
        }

        public String toString() {
            return "MockPage(id=" + pid + ", datum=" + datum + ")";
        }
    }

    /**
     * Simple page maker for MockPage objects
     */
    public static class MockPageMaker implements PageMaker {
        @Override
        public Page makePage(PageId pid, byte[] bytes) {
            return new MockPage(pid, bytes);
        }

        @Override
        public Page makePage(PageId pid) {
            return new MockPage(pid, -1);
        }

    }
}
