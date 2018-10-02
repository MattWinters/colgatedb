package colgatedb.page;

import colgatedb.tuple.TupleDesc;

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
public class SlottedPageMaker implements PageMaker {

    private final TupleDesc td;
    private final int pageSize;

    public SlottedPageMaker(TupleDesc td, int pageSize) {
        this.td = td;
        this.pageSize = pageSize;
    }

    @Override
    public Page makePage(PageId pid, byte[] bytes) {
        return new SlottedPage(pid, td, pageSize, bytes);
    }

    @Override
    public Page makePage(PageId pid) {
        return new SlottedPage(pid, td, pageSize);
    }
}
