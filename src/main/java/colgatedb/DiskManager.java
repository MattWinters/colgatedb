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

/**
 * DiskManager is responsible for reading/writing pages from/to disk.  It also supports
 * allocation of new pages.  Page deallocation is currently NOT supported.  This is mainly
 * to reduce complexity and because practically speaking, databases tend to only grow over
 * time.  (Recall that with many files of records, since new pages are only needed if
 * all existing pages are full, page allocations are only necessary after a significant
 * number of records is added to the database.)
 */
public interface DiskManager {

    /**
     * Allocate space on disk for a new page
     * @param pid caller is responsible for supplying appropriate PageId for the page about to be created,
     *            typically a page for an existing table (with a known tableid) with a page number that is
     *            one larger than the largest page number for that table.
     */
    void allocatePage(PageId pid);

    /**
     * Read a page from disk and create an in-memory representation (Page object).
     * @param pid of the desired page
     * @param pageMaker object capable of building page from bytes
     * @return Page object
     */
    Page readPage(PageId pid, PageMaker pageMaker);

    /**
     * Write an in-memory Page object to disk.  Uses {@link Page#getId()} to determine where page
     * should be written.
     * @param page to write to disk.
     */
    void writePage(Page page);
}