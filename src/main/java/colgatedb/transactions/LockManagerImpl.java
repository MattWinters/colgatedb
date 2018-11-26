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
    private HashMap<PageId, LockTableEntry> tableEntries;
    private LockTableEntry tableEntry;
    private HashMap<TransactionId, Node> dependencyGraph = new HashMap<TransactionId, Node>();


    public LockManagerImpl() {
        tableEntries = new HashMap<>();
    }

        // make entry then add yourself to the queue
    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        //The threads will wait in this loop until they can acquire a lock
        while (waiting) {
            //make sure that only one thread can try to acquire a lock at a time.
            synchronized (this) {
                boolean canAcquire = false;
                if (!tableEntries.containsKey(pid)){
                    tableEntry = new LockTableEntry();
                    tableEntries.put(pid, tableEntry);

                }
                tableEntry = tableEntries.get(pid);
                //Upgrades get added to the front of the queue
                if(tableEntry.getLockHolders().contains(tid) && tableEntry.getLockType().equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_WRITE)){
                    tableEntry.addRequest(tid, perm, true);
                }
                else {
                    tableEntry.addRequest(tid, perm, false);
                }
                //Check if the lock can be acquired
                if (tableEntry.getNextTransaction() == tid ){
                    if (tableEntry.getLockType() == null){
                        canAcquire = true;
                    }
                    else if (tableEntry.getLockHolders().size() == 1 && tableEntry.getLockHolders().contains(tid)){
                        canAcquire = true;
                    }
                    else if (perm.equals(Permissions.READ_ONLY) && tableEntry.getLockType().equals(Permissions.READ_ONLY)){
                        canAcquire = true;
                    }
                }
                //If the lock can be acquired, acquire the lock and update the dependency graph removing all edges from the tid thats no longer waiting
                //Tell the manager that this lock is no longer waiting so it will break the loop and notify all other threads
                if (canAcquire){
                    dependencyGraph.put(tid, new Node(tid, new ArrayList<>()));
                    tableEntry.setLock(tid, perm);
                    waiting = false;
                    notifyAll();
                }
                //If the lock can't be acquired check if it will cause deadlock by seeing if it can be added to the dependency graph
                //If it can't be added clean up the tid's request and the graph then abort the current tid
                else{
                    if (! canAddToDependencyGraph(tid, tableEntry )){
                        tableEntry.releaseRequest(tid, perm);
                        dependencyGraph.replace(tid, new Node (tid, new ArrayList<>()));
                        throw new TransactionAbortedException();
                    }
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
        List<TransactionId> tids = new ArrayList<>();
        tids.addAll(tableEntry.getLockHolders());
        return tids;
    }
    /*
    This method will take the tid and the table entry and update the dependency graph adding all of the locks that are preventing
    the current tid from receiving the requested lock.
    Once the graph has been updated the method will check if the addition to the graph creates deadlock
     */
    public boolean canAddToDependencyGraph(TransactionId tid, LockTableEntry tableEntry){
        Set<TransactionId> lockHolders = tableEntry.getLockHolders();
        ArrayList<Node> lockedTids = new ArrayList<Node>();
        Node current;
        //Create a list of all of the tids that are holding locks that the current tid needs
        for (TransactionId lockHoldingTid : lockHolders){
            lockedTids.add(dependencyGraph.get(lockHoldingTid));
        }
        if (dependencyGraph.containsKey(tid)){
            current = dependencyGraph.get(tid);
            current.dependencyList.addAll(lockedTids);
            dependencyGraph.replace(tid, current);
        }
        else {
            current = new Node(tid, lockedTids);
            dependencyGraph.put(tid, current);
        }
        //Check if the addition of the new request creates deadlock
        return checkForDeadlock(current, current);
    }

    /*
    The method uses a Depth First Search Algorithm to check weather or not there is a cycle in the dependency graph.
    If there is a cycle then deadlock has been detected.
    The function will return a boolean corresponding to weather or not deadlock was detected.
     */
    public boolean checkForDeadlock(Node start, Node current){
        ArrayList<Node> children = current.dependencyList;
        // The for loop iterates over each child of the current node and checks if one of the children is the start node.
        // The loop will recursively call the function for each child if it is not the start node.
        for (Node child : children){
            if (child.tid == start.tid){
                return true;
            }
            else {
                return checkForDeadlock(start, child);
            }
        }
        return false;
    }

    class Node {
        TransactionId tid;
        ArrayList<Node> dependencyList;

        public Node (TransactionId tid, ArrayList<Node> dependencyList){
            this.tid = tid;
            this.dependencyList = dependencyList;
        }

        public String toString() {
            String str = "" + tid + "\t";
            str = str + "dependency List: " + "\n \t";
            for (Node cur : dependencyList) {
                str = str + cur.tid + "\n";
            }
            return str;
        }
    }
}
