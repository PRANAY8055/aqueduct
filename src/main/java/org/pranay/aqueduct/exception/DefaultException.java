package org.pranay.aqueduct.exception;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class DefaultException extends Exception {
    public DefaultException(String message) {
        super(message);
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
