package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

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
public interface BufferManager {

    /**
     * Retrieves desired page, from disk if necessary, and increments pin count.
     *
     * @param pid pid of desired page
     * @param pageMaker used to create Page object if it must be read from disk
     * @return Page object
     */
    Page pinPage(PageId pid, PageMaker pageMaker);

    /**
     * Decrements pin count on page and updates its dirty status.
     * @param pid pid of page to unpin
     * @param isDirty whether or not the user of this page dirtied it
     * @throws BufferManagerException if pid is not in the cache or pin count is already zero
     */
    void unpinPage(PageId pid, boolean isDirty);

    /**
     * Flush page to disk.  Page should only be flushed if it is dirty.
     * @param pid pid of page to flush
     */
    void flushPage(PageId pid);

    /**
     * Flush all pages to disk.  Pages should only be flushed if they are dirty.
     */
    void flushAllPages();

    /**
     * Sets flag on buffer manager.
     * <p>
     * If this method is called and allowEvictDirty is to false, then the Buffer Manager should
     * only consider undirtied pages when looking for a page to evict.  On the other hand, if
     * this method is called and allowEvictDirty is to true, then the Buffer Manager should
     * consider dirty pages as candidates for eviction.
     * @param allowEvictDirty indicates whether or not dirty pages are candidates for eviction.
     */
    void evictDirty(boolean allowEvictDirty);

    /**
     * Request that disk manager allocates another page.
     * @param pid the pid of a new page to be allocated
     */
    void allocatePage(PageId pid);

    /**
     * @param pid pid of desired page
     * @return true if the page with the given pid is dirty.  If the page is not there or is
     * not dirty returns false.
     */
    boolean isDirty(PageId pid);

    /* ----- the remaining methods should be used with caution ----- */
    /*
       Other components of ColgateDB require additional control over the
       buffer pool.

       For example, the Recovery Manager may need to discard
       a page from the buffer pool because the transaction that modified
       the page was aborted and so its changes need to be undone.
     */

    /**
     * @param pid pid of desired page
     * @return returns true if the page with given pid resides in buffer pool
     */
    boolean inBufferPool(PageId pid);

    /**
     * Gets a page from the buffer pool if it's there, otherwise throws an exception.
     * @param pid pid of desired page
     * @return the page if it is in the cache
     * @throws BufferManagerException if the page is not in cache
     */
    Page getPage(PageId pid);

    /**
     * Removes the specific page from the buffer pool if it is there. The page
     * should NOT be flushed to disk, but simply removed from the pool.  If the page is
     * dirty, then those modifications will be lost.  If the page is not in the buffer
     * pool then this method should do nothing.
     * <p>
     * This should remove the page even if the page is not marked as dirty (because it
     * may be the case that the page has been dirtied but the transaction that dirtied
     * was abort before it had the chance to unpin the page and therefore mark it as dirty).
     * @param pid pid of desired page
     */
    void discardPage(PageId pid);

}
