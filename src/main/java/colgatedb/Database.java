package colgatedb;

import colgatedb.logging.LogFile;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicReference;

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
 * Database is a class that initializes several static variables used by the
 * database system (the catalog, disk manager, buffer manager, log files, etc.)
 * <p/>
 * Provides a set of methods that can be used to access these variables from
 * anywhere (singleton design pattern).
 * <p>
 * For example, to invoke the pinPage method on the Buffer Manager, one can do
 * this:
 * <code>Database.getBufferManager().pinPage(pid, pageMaker)</code>
 */
public class Database {

    // default settings
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int DEFAULT_POOL_SIZE = 10;   // number of pages in buffer pool

    // actual settings
    private static int pageSize = DEFAULT_PAGE_SIZE;
    private static int poolSize = DEFAULT_POOL_SIZE;

    private static AtomicReference<Database> _instance = new AtomicReference<Database>(new Database());
    private final Catalog _catalog;
    private DiskManagerImpl _diskManager;
    private BufferManager _bufferManager;
    private AccessManager _accessManager;

    private final static String LOGFILENAME = "log";
    private LogFile _logfile;

    /**
     * Constructor is private: ensures only one instance of Database (singleton design pattern).
     */
    private Database() {
        _diskManager = new DiskManagerImpl(pageSize);
        _catalog = new Catalog(pageSize, _diskManager);
        _bufferManager = new BufferManagerImpl(poolSize, _diskManager);

        AccessManager tmpAM = null;
        try {
            Class<?> logFileClass = Class.forName("colgatedb.AccessManagerImpl");
            Constructor<?> constructor = logFileClass.getDeclaredConstructors()[0];
            tmpAM = (AccessManager) constructor.newInstance(_bufferManager);
        } catch (ClassNotFoundException | InvocationTargetException |
                IllegalAccessException | InstantiationException e) {
            System.err.println("Warning: unable to initialize access manager");
        }
        _accessManager = tmpAM;

        LogFile tmpLF = null;
        try {
            Class<?> logFileClass = Class.forName("colgatedb.logging.LogFileImpl");
            Constructor<?> constructor = logFileClass.getDeclaredConstructors()[0];
            tmpLF = (LogFile) constructor.newInstance(new File(LOGFILENAME));
        } catch (ClassNotFoundException | InvocationTargetException |
                IllegalAccessException | InstantiationException e) {
            System.err.println("Warning: unable to initialize log file");
        }
        _logfile = tmpLF;
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static DiskManagerImpl getDiskManager() {
        return _instance.get()._diskManager;
    }

    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    public static BufferManager getBufferManager() {
        return _instance.get()._bufferManager;
    }

    public static AccessManager getAccessManager() {
        if (_instance.get()._accessManager == null) {
            throw new DbException("Access manager was never initialized!");
        }
        return _instance.get()._accessManager;
    }

    public static LogFile getLogFile() {
        if (_instance.get()._logfile == null) {
            throw new DbException("Log file was never initialized!");
        }
        return _instance.get()._logfile;
    }


    // ----------------- methods below are primarily used for testing ------------------------
    public static AccessManager resetBufferPool(int numPages) {
        _instance.get()._bufferManager = new BufferManagerImpl(numPages,
                _instance.get()._diskManager);
        _instance.get()._accessManager = null;
        try {
            Class<?> logFileClass = Class.forName("colgatedb.AccessManagerImpl");
            Constructor<?> constructor = logFileClass.getDeclaredConstructors()[0];
            _instance.get()._accessManager = (AccessManager) constructor.newInstance(_instance.get()._bufferManager);
        } catch (ClassNotFoundException | InvocationTargetException |
                IllegalAccessException | InstantiationException e) {
            System.err.println("Warning: unable to initialize log file");
        }
        return _instance.get()._accessManager;
    }
    public static AccessManager resetBufferPool() {
        return resetBufferPool(poolSize);
    }

    // reset the database, used for unit tests only.
    public static void reset() {
        pageSize = DEFAULT_PAGE_SIZE;
        poolSize = DEFAULT_POOL_SIZE;
        _instance.set(new Database());
    }

    public static void setPageSize(int pageSize) {
        Database.pageSize = pageSize;
        _instance.set(new Database());
    }

    public static void setBufferPoolSize(int numPages) {
        poolSize = numPages;
        _instance.set(new Database());
    }

    // -- new: added on 12/1/16
    public static void setDiskManager(DiskManagerImpl diskManager) {
        _instance.get()._diskManager = diskManager;
    }

    public static void setBufferManager(BufferManager bufferManager) {
        _instance.get()._bufferManager = bufferManager;
    }

    public static void setAccessManager(AccessManager accessManager) {
        _instance.get()._accessManager = accessManager;
    }

    public static void setLogFile(LogFile lf) {
        _instance.get()._logfile = lf;
    }
}
