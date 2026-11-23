package org.pranay.aqueduct.sinkdecorator;

import org.pranay.aqueduct.metrics.AqueductInstrumentation;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackOff {

    private AqueductInstrumentation aqueductInstrumentation;

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            aqueductInstrumentation.captureNonFatalError("aqueduct_error_event", e, "Backoff thread sleep for {} milliseconds interrupted : {} {}",
                    milliseconds, e.getClass(), e.getMessage());
        }
    }
}
