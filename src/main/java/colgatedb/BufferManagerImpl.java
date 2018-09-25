package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.HashMap;
import java.util.Map;

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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction

    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        throw new UnsupportedOperationException("implement me!");
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void flushPage(PageId pid) {
    }

    @Override
    public synchronized void flushAllPages() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}