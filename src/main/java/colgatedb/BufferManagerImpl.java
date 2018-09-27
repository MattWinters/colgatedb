package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.ArrayList;
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
    private int numPages;
    private DiskManager dm;
    private HashMap<PageId, Frame> cache = new HashMap<PageId, Frame>();

    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        this.numPages = numPages;
        this.dm = dm;
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        Page page;
        Frame frame = cache.get(pid);
        if (frame == null){
            page = dm.readPage(pid, pageMaker);
            frame = new Frame(page);
            cache.put(pid, frame);
        }
        else {
            page = frame.page;
        }
        frame.pinCount ++;
        return page;
    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {

//        if (pinCount == 0){
//            throw new BufferManagerException ("Pin Count is already 0");
//        }
//        //Checks if page is in the cache, if it isn't in the cache it will throw and exception
//        //Page page = getPage(pid);
//
//        Page p = cache.get(pid);
//        if (p == null){
//            throw new BufferManagerException ("page is not in the cache");
//        }
//        pinCount --;
//        isDirty = true;

    }

    @Override
    public synchronized void flushPage(PageId pid) {
//        Page page = cache.get(pid);
//        dm.writePage(page);
    throw new UnsupportedOperationException("implement me!");
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