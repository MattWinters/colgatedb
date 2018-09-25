package colgatedb;

public class BufferManagerException extends RuntimeException {
    public BufferManagerException(String message) {
        super(message);
    }

    public BufferManagerException(Exception e) {
        super(e);
    }
}
