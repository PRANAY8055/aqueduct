package org.pranay.aqueduct.exception;

public class SinkTaskFailedException extends RuntimeException {
    public SinkTaskFailedException(Throwable throwable) {
        super(throwable);
    }
}
