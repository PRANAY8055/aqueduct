package org.pranay.aqueduct.exception;

public class AqueductConsumerFailedException extends RuntimeException {
    public AqueductConsumerFailedException(Throwable th) {
        super(th);
    }
}
