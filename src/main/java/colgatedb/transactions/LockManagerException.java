package colgatedb.transactions;

/**
 * Created by mhay on 8/12/16.
 */
public class LockManagerException extends RuntimeException {
    public LockManagerException(String message) {
        super(message);
    }
    public LockManagerException(Exception e) {
        super(e);
    }
}
