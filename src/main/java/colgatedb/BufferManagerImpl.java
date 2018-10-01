package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
    private LinkedList<PageId> queue = new LinkedList<PageId>();

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
            if (numPages <= cache.size()){
                evict();
            }
            page = dm.readPage(pid, pageMaker);
            frame = new Frame(page);
            cache.put(pid, frame);
        }
        else {
            page = frame.page;
        }
        frame.pinCount ++;
        //frame.isDirty = true;
        cache.replace(pid, frame);
        queue.add(pid);
        return page;
    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {

        //Checks if page is in the cache, if it isn't in the cache it will throw and exception
        Frame frame = cache.get(pid);
        if (frame == null){
            throw new BufferManagerException ("page is not in the cache");
        }
        if (frame.pinCount == 0){
            throw new BufferManagerException ("Pin Count is already 0");
        }


        //!!!!!!!!!!!! NOT SURE IF THIS IS WHAT i'M SUPPOSED TO DO ASK!!!!!!!!!!!
        if (!frame.isDirty) {
            frame.isDirty = isDirty;
        }
        frame.pinCount --;
        cache.replace(pid,frame);
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        Frame frame = getFrame(pid);
        if (frame.isDirty){
            dm.writePage(frame.page);
            frame.isDirty = false;
            cache.replace(pid, frame);
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for(PageId pid : cache.keySet()){
            flushPage(pid);
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    public synchronized boolean evict(){
        for (PageId pid : cache.keySet()){
            Frame frame = getFrame(pid);
            if (this.allowEvictDirty || !isDirty(pid)){
                if (frame.pinCount <= 0) {
                    dm.writePage(frame.page);
                    cache.remove(pid, frame);
                    return true;
                }
            }
        }
        throw new BufferManagerException ("there is no page to evict");
    }


    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        Frame frame = cache.get(pid);
        if (frame == null){
            return false;
        }
        return frame.isDirty;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        Frame frame = cache.get(pid);
        return (frame != null);
    }



    public synchronized Frame getFrame(PageId pid) {
        Frame frame = cache.get(pid);
        if (frame == null){
            throw new BufferManagerException ("page is not in the cache");
        }
        else {
            return frame;
        }
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        Frame frame = cache.get(pid);
        if (frame == null){
            throw new BufferManagerException ("page is not in the cache");
        }
        else {
            return frame.page;
        }
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        if(inBufferPool(pid)) {
            cache.remove(pid);
        }
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
            this.pinCount = 0;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}