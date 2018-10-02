package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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

/**
 * This DiskManager uses OS files to store data.
 *
 * DiskManager depends in a deep way on the structure of PageId.
 * Since each PageId consists of a tableid and a pageno, it seems
 * reasonable to store data to disk using a file for each tableid
 * and then storing pages sequentially according to pageno.  This is
 * exactly what this DiskManager does.
 *
 * The DiskManager is NOT responsible for persisting the mapping between
 * tableid and OS file.  This is the responsibility of the {@link Catalog}.  Whenever
 * a DiskManagerImpl instance is created, the creator is responsible for calling
 * {@link #addFileEntry(int, String)} to update the DiskManager's local mapping.
 */
public class DiskManagerImpl implements DiskManager {

    private static final String MODE = "rws";
    private final int pageSize;
    Map<Integer, String> filenames = new HashMap<Integer, String>(); // local mapping from tableid to OS filename

    public DiskManagerImpl(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Add entry in map between tableid and corresponding OS file.  This
     * method should be called when the database is first being brought "online."
     * @param tableid
     * @param filename
     */
    public void addFileEntry(int tableid, String filename) {
        filenames.put(tableid, filename);
        File file = new File(filename);
        // check if file already exists
        if (!file.isFile()) {
            try {
                RandomAccessFile dataFile = new RandomAccessFile(file, MODE);
                dataFile.close();
            } catch (IOException e) {
                throw new DiskManagerException(e);
            }
        }
    }

    public int getNumPages(int tableid) {
        File file = new File(filenames.get(tableid));
        long length = file.length();
        if (length % pageSize != 0) {
            throw new DiskManagerException("Invalid Length");
        }
        return (int) (length / pageSize);
    }

    public void allocatePage(PageId pid) {
        // check that page being allocated is next page in file
        int pagenoRequested = pid.pageNumber();
        int numPages = getNumPages(pid.getTableId());
        if (pagenoRequested < numPages) {
            throw new DiskManagerException("Attempting to allocate a page that already exists!" +
            " You requested that page " + pagenoRequested + " be allocated but file has " + numPages + "pages.");
        } else if (pagenoRequested > numPages) {
            throw new DiskManagerException("Attempting to allocate pageno = " + pagenoRequested +
                    " but file currently has only " + numPages + " pages.");
        }
        byte[] emptyBytes = new byte[pageSize];
        writePageData(pid, emptyBytes);
    }

    public Page readPage(PageId pid, PageMaker pageMaker) {
        byte[] bytes = readPageData(pid);
        return pageMaker.makePage(pid, bytes);
    }

    public void writePage(Page page) {
        PageId pid = page.getId();
        byte[] pageData = page.getPageData();
        writePageData(pid, pageData);
    }

    private void writePageData(PageId pid, byte[] pageData) {
        if (pageData.length != pageSize) {
            throw new DiskManagerException("page size is invalid! Got " + pageData.length + " bytes, expected " + pageSize);
        }
        File file = lookupFile(pid);
        try {
            RandomAccessFile dataFile = new RandomAccessFile(file, MODE);
            int offset = pid.pageNumber() * pageSize;
            if (offset > dataFile.length()) {
                throw new RuntimeException("Writing a page beyond end of file");
            }
            dataFile.seek(offset);
            dataFile.write(pageData);
            dataFile.close();
        } catch (IOException e) {
            throw new DiskManagerException(e);
        }
    }

    private byte[] readPageData(PageId pid) {
        File file = lookupFile(pid);
        try {
            RandomAccessFile dataFile = new RandomAccessFile(file, MODE);
            if (dataFile.length() < pageSize * pid.pageNumber() + pageSize) {
                throw new DiskManagerException("Attempting to read beyond end of file!");
            }
            dataFile.seek(pageSize * pid.pageNumber());
            byte[] data = new byte[pageSize];
            dataFile.read(data);
            dataFile.close();
            return data;
        } catch (IOException e) {
            throw new DiskManagerException(e);
        }
    }

    private File lookupFile(PageId pid) {
        if (!filenames.containsKey(pid.getTableId())) {
            throw new DiskManagerException("No record of this table id!");
        }
        return new File(filenames.get(pid.getTableId()));
    }

}
