package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.*;

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
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    // some suggested private instance variables; feel free to modify
    private Permissions lockType;             // null if no one currently has a lock
    private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private List<LockRequest> requests;       // a queue of outstanding requests
    private LockRequest request;

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
        // you may wish to add statements here.
    }

    public Permissions getLockType() {
        return lockType;
    }

    public Set<TransactionId> getLockHolders() {
        return lockHolders;
    }

    public List<LockRequest> getRequests() {
        return requests;
    }

    public TransactionId getNextTransaction(){
        request = requests.get(0);
        return request.getTid();
    }

    public void releaseLock (TransactionId tid) {
        lockHolders.remove(tid);
        if (lockHolders.size() == 0) {
            lockType = null;
        }
    }

    public void setLock (TransactionId tid, Permissions perm){
        if (lockHolders.contains(tid)){
            lockHolders.remove(tid);
        }
        request = new LockRequest(tid, perm);
        lockHolders.add(tid);
        lockType = perm;
        requests.remove(request);
    }

    public void addRequest (TransactionId tid, Permissions perm , boolean upgrade){
        request = new LockRequest(tid, perm);
        if (upgrade){
            requests.add(0, request);
        }
        if (!requests.contains(request)) {
            requests.add(request);
        }

    }

//    public boolean alreadyRequested (TransactionId tid, Permissions perm){
//        LockRequest request = new LockRequest(tid, perm);
//        if (request.contains)
//    }

    public String toString(){
        String str = "";
        str = str + "perm = " + lockType + "\n";
        str = str + "lockHolders = " + lockHolders.toString() + "\n";
        str = str + "requests = " + requests.toString() + "\n";
        str = str + "request = " + request.toString();
        return str;
    }


    // you may wish to implement methods here.

    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    private class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public TransactionId getTid(){
            return tid;
        }

        public Permissions getPermision() {
            return perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
