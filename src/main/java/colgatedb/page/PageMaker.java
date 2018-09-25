package colgatedb.page;

/**
 * Created by mhay on 6/8/16.
 */
public interface PageMaker {

    Page makePage(PageId pid, byte[] bytes);

    // makes empty page, used in testing only
    Page makePage(PageId pid);
}
