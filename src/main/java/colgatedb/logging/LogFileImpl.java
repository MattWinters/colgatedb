
package colgatedb.logging;

import colgatedb.Database;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.transactions.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 LogFile implements the recovery subsystem of SimpleDb.  This class is
 able to write different log records as needed, but it is the
 responsibility of the caller to ensure that write ahead logging and
 two-phase locking discipline are followed.  <p>

 <u> Locking note: </u>
 <p>

 Many of the methods here are synchronized (to prevent concurrent log
 writes from happening); many of the methods in BufferPool are also
 synchronized (for similar reasons.)  Problem is that BufferPool writes
 log records (on page flushed) and the log file flushes BufferPool
 pages (on checkpoints and recovery.)  This can lead to deadlock.  For
 that reason, any LogFile operation that needs to access the BufferPool
 must not be declared synchronized and must begin with a block like:

 <p>
 <pre>
 synchronized (Database.getBufferPool()) {
 synchronized (this) {

 ..

 }
 }
 </pre>
 */

/**
 * <p> The format of the log file is as follows:
 * <p/>
 * <ul>
 * <p/>
 * <li> The first long integer of the file represents the offset of the
 * last written checkpoint, or -1 if there are no checkpoints
 * <p/>
 * <li> All additional data in the log consists of log records.  Log
 * records are variable length.
 * <p/>
 * <li> Each log record begins with an integer type and a long integer
 * transaction id.
 * <p/>
 * <li> Each log record ends with a long integer file offset representing
 * the position in the log file where the record began.
 * <p/>
 * <li> There are six record types: ABORT, COMMIT, UPDATE, BEGIN,
 * CHECKPOINT, and CLR
 * <p/>
 * <li> ABORT, COMMIT, and BEGIN records contain no additional data
 * <p/>
 * <li>UPDATE RECORDS consist of two entries, a before image and an
 * after image.  These images are serialized Page objects, and can be
 * accessed with the LogFile.readPageData() and LogFile.writePageData()
 * methods.  See {@link LogFileRecovery#print()} for an example.
 * <p/>
 * <li>CLR RECORDS consist of one entry, an after image.  CLR stands for
 * compensating log record and it is written during undo phase of rollback
 * and recovery.
 * <p/>
 * <li> CHECKPOINT records consist of active transactions at the time
 * the checkpoint was taken and their first log record on disk.  The format
 * of the record is an integer count of the number of transactions, as well
 * as a long integer transaction id for each active transaction.
 * <p/>
 * </ul>
 *
 * @author mhay, adapted from Madden
 */

public class LogFileImpl implements LogFile {

    final File logFile;
    private RandomAccessFile raf;
    private LogFileRecovery logFileRecovery;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    private Set<Long> activeTids = new HashSet<Long>();

    /**
     * Constructor.
     * Initialize and back the log file with the specified file.
     * We're not sure yet whether the caller is creating a brand new DB,
     * in which case we should ignore the log file, or whether the caller
     * will eventually want to recover (after populating the Catalog).
     * So we make this decision lazily: if someone calls recover(), then
     * do it, while if someone starts adding log file entries, then first
     * throw out the initial log file contents.
     *
     * @param f The log file's name
     */
    public LogFileImpl(File f) throws IOException {
        this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;
        logFileRecovery = new LogFileRecovery(new RandomAccessFile(logFile, "r"));


        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
        // public void run() { shutdown(); }
        // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    void preAppend() throws LogManagerException {
        try {
            // we're about to append a log record. if we weren't sure whether the
            // DB wants to do recovery, we're sure now -- it didn't. So truncate
            // the log.
            if (recoveryUndecided) {
                recoveryUndecided = false;
                raf.seek(0);
                raf.setLength(0);
                raf.writeLong(NO_CHECKPOINT_ID);
                raf.seek(raf.length());
            }
            // we're about to append a log record... make sure we're at the end of the log!
            if (raf.getFilePointer() != raf.length()) {
                throw new RuntimeException("About to append to log file but not" +
                        " located at end of log!  Risk overwriting log data!");
            }
        } catch (IOException e) {
            throw new LogManagerException(e);
        }
    }

    private void checkActive(TransactionId tid, boolean shouldBeActive) throws LogManagerException {
        // should check for active but many test cases do not explicitly start and stop
        // transactions and so checking for active can cause tests to fail
        // need to revise this eventually
    }

    /**
     * Write a BEGIN record for the specified transaction
     *
     * @param tid The transaction that is beginning
     */
    @Override
    public synchronized void logXactionBegin(TransactionId tid)
            throws IOException {
        checkActive(tid, false);
        preAppend();
        Long recordStart = raf.getFilePointer();
        raf.writeInt(LogType.BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(recordStart);
        activeTids.add(tid.getId());
    }

    /**
     * Write a commit record to disk for the specified tid,
     * and force the log to disk.
     *
     * @param tid The committing transaction.
     */
    @Override
    public synchronized void logCommit(TransactionId tid) throws IOException {
        //should we verify that this is a live transaction?
        checkActive(tid, true);
        preAppend();

        Long recordStart = raf.getFilePointer();
        raf.writeInt(LogType.COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(recordStart);
        force();
        activeTids.remove(tid.getId());
    }

    /**
     * Perform a rollback which should cause an abort to be written
     * to log.
     *
     * @param tid The aborting transaction.
     */
    @Override
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getAccessManager()) {

            synchronized (this) {
                //should we verify that this is a live transaction?
                checkActive(tid, true);

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                force();
                logFileRecovery.rollback(tid);
            }
        }
    }

    /**
     * Write a commit record to disk for the specified tid,
     * and force the log to disk.
     *
     * @param tid The committing transaction.  Need long because may
     *            not have a live transaction.
     */
    @Override
    public synchronized void logAbort(Long tid) throws IOException {
        preAppend();
        Long recordStart = raf.getFilePointer();
        raf.writeInt(LogType.ABORT_RECORD);
        raf.writeLong(tid);
        raf.writeLong(recordStart);
        force();
        activeTids.remove(tid);
    }

    /**
     * Write an UPDATE record to disk for the specified tid and page
     * (with provided         before and after images.)
     *
     * @param tid    The transaction performing the write
     * @param before The before image of the page
     * @param after  The after image of the page
     * @see Page#getBeforeImage
     */
    @Override
    public synchronized void logWrite(TransactionId tid, Page before,
                                      Page after)
            throws LogManagerException {
        checkActive(tid, true);
        preAppend();
        /* update record consists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */

        try {
            Long recordStart = raf.getFilePointer();
            raf.writeInt(LogType.UPDATE_RECORD);
            raf.writeLong(tid.getId());
            writePageData(raf, before);
            writePageData(raf, after);
            raf.writeLong(recordStart);
        } catch (IOException e) {
            throw new LogManagerException(e);
        }
    }

    /**
     * Write a CLR record to disk for the specified tid and page
     * (with provided after image.)
     *
     * @param tid    The transaction performing the write
     * @param after  The after image of the page
     * @see Page#getBeforeImage
     */
    @Override
    public synchronized void logCLR(TransactionId tid, Page after)
            throws IOException {
        logCLR(tid.getId(), after);
    }

    /**
     * Write a CLR record to disk for the specified tid and page
     * (with provided after image.)
     *
     * @param tid    The transaction performing the write. Need
     *               long because may not have a live transaction.
     * @param after  The after image of the page
     * @see Page#getBeforeImage
     */
    @Override
    public synchronized void logCLR(Long tid, Page after)
            throws IOException {

        // transaction may be active or we may be in recovery mode
        preAppend();
        /* update record consists of

           record type
           transaction id
           after page data (see writePageData)
           start offset
        */
        Long recordStart = raf.getFilePointer();
        raf.writeInt(LogType.CLR_RECORD);
        raf.writeLong(tid);
        writePageData(raf, after);
        raf.writeLong(recordStart);
    }


    static void writePageData(RandomAccessFile raf, Page p) throws IOException {
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
    }

    static Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i < numIdArgs; i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId) idConsts[0].newInstance(idArgs);

            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            Constructor<?> constructor = pageClass.getConstructor(new Class[]{PageId.class, byte[].class});
            newPage = (Page) constructor.newInstance(pageArgs);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return newPage;
    }

    /**
     * Checkpoint the log and write a checkpoint record.
     */
    @Override
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getAccessManager()) {
            synchronized (this) {
                preAppend();
                long startCpOffset, endCpOffset;

                force();
                Database.getBufferManager().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(LogType.CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(activeTids.size());
                for (Long key : activeTids) {
                    raf.writeLong(key);
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(startCpOffset);
            }
        }


        logTruncate();
    }

    /**
     * Truncate any unneeded portion of the log to reduce its space
     * consumption
     */
    @Override
    public synchronized void logTruncate() throws IOException {
        // this is optional
    }

    /**
     * Shutdown the logging system, writing out whatever state
     * is necessary so that start up can happen quickly (without
     * extensive recovery.)
     */
    @Override
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     */
    @Override
    public void recover() throws IOException {
        synchronized (Database.getAccessManager()) {
            synchronized (this) {
                recoveryUndecided = false;
                raf.seek(raf.length());      // go to end of log file
                logFileRecovery.recover();
            }
        }
    }

    @Override
    public synchronized void force() throws LogManagerException {
        try {
            raf.getChannel().force(true);
        } catch (IOException e) {
            throw new LogManagerException(e);
        }
    }

}
