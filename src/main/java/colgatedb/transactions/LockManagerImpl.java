package colgatedb.transactions;

import colgatedb.page.PageId;

import javax.sound.midi.Soundbank;
import java.util.*;
import java.util.concurrent.locks.Lock;

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
public class LockManagerImpl implements LockManager {
    private boolean isExclusive;
    private boolean isShared;
    private boolean inUse = false;
    private int permLevel = -1;
    private HashMap<PageId, LockTableEntry> tableEntries;
    private LockTableEntry tableEntry;

    public LockManagerImpl() {
        tableEntries = new HashMap<>();
    }

        // make entry then add yourself to the queue
    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        while (waiting) {
            System.out.println("tid :" + tid + " waiting");
            synchronized (this) {
                boolean canAquire = false;
                if (!tableEntries.containsKey(pid)){
                    tableEntry = new LockTableEntry();
                    tableEntries.put(pid, tableEntry);

                }
                tableEntry = tableEntries.get(pid);

                //Upgrades get added to the front of the queue
                if(tableEntry.getLockHolders().contains(tid) && tableEntry.getLockType().equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_WRITE)){
                    System.out.println("upgrading!");
                    System.out.println(tableEntry.toString());
                    tableEntry.addRequest(tid, perm, true);
                    System.out.println(tableEntry.toString());
                }
                else {
                    tableEntry.addRequest(tid, perm, false);
                    System.out.println(tableEntry.toString());
                }
                //Check if the lock can be acquired
                if (tableEntry.getNextTransaction() == tid ){
                    if (tableEntry.getLockType() == null){
                        canAquire = true;
                    }
                    else if (tableEntry.getLockHolders().size() == 1 && tableEntry.getLockHolders().contains(tid)){
                        canAquire = true;
                    }
                    else if (perm.equals(Permissions.READ_ONLY) && tableEntry.getLockType().equals(Permissions.READ_ONLY)){
                        canAquire = true;
                    }

                }

                if (canAquire){
                    tableEntry.setLock(tid, perm);
                    waiting = false;
                    System.out.println("tid :" + tid + " acquire lock");
                    System.out.println(tableEntry.toString());
                }

                if (waiting) {
                    try {
                        wait();
                    } catch (InterruptedException e) {}
                }
            }
        }
    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        if (!tableEntries.containsKey(pid)){
            return false;
        }
        tableEntry = tableEntries.get(pid);
        Set<TransactionId> lockHolders = tableEntry.getLockHolders();
        return (lockHolders.contains(tid) && tableEntry.getLockType().permLevel >=  perm.permLevel);
    }


    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!tableEntries.containsKey(pid)){
            throw new LockManagerException ("pid is not in the table");
        }
        tableEntry = tableEntries.get(pid);
        if (! tableEntry.getLockHolders().contains(tid)){
            throw new LockManagerException ("pid is not in the table");
        }
        tableEntry.releaseLock(tid);
        notifyAll();
        System.out.println("tid :" + tid + " release lock");
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        List<PageId> pidsToReturn = new ArrayList<>();
        Set<PageId> pages = tableEntries.keySet();
        for (PageId pid : pages){
            tableEntry = tableEntries.get(pid);
            Set<TransactionId> lockHolders = tableEntry.getLockHolders();
            if (lockHolders.contains(tid)){
                pidsToReturn.add(pid);
            }
        }
        return pidsToReturn;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        tableEntry = tableEntries.get(pid);
        //####might just need to make a list and use .addall()###
        List<TransactionId> tids = new ArrayList<>();
        tids.addAll(tableEntry.getLockHolders());
        return tids;
    }

    @Override
    public LockTableEntry getTableEntry() {
        return tableEntry;
    }
}
