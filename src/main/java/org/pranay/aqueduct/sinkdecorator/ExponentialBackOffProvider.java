package org.pranay.aqueduct.sinkdecorator;

import org.pranay.aqueduct.metrics.AqueductInstrumentation;

import static org.pranay.aqueduct.metrics.Metrics.RETRY_SLEEP_TIME_MILLISECONDS;
import static java.lang.Math.toIntExact;

/**
 * ExponentialBackOffProvider is a provider of exponential back-off algorithm.
 * <p>
 * The backoff time is computed as per the following formula.
 * Min{maximumExpiryTimeInMS, initialExpiryTimeInMs * Math.pow(backoffRate,
 * attemptCount)}
 */
public class ExponentialBackOffProvider implements BackOffProvider {

    private final int initialExpiryTimeInMs;
    private final int backoffRate;
    private final int maximumExpiryTimeInMS;
    private AqueductInstrumentation aqueductInstrumentation;
    private final BackOff backOff;

    /**
     * Instantiates a new Exponential back off provider.
     *
     * @param initialExpiryTimeInMs the initial expiry time in ms
     * @param backoffRate           the backoff rate
     * @param maximumExpiryTimeInMS the maximum expiry time in ms
     * @param aqueductInstrumentation       the instrumentation
     * @param backOff               the back off
     */
    public ExponentialBackOffProvider(int initialExpiryTimeInMs, int backoffRate, int maximumExpiryTimeInMS,
                                      AqueductInstrumentation aqueductInstrumentation, BackOff backOff) {
        this.initialExpiryTimeInMs = initialExpiryTimeInMs;
        this.backoffRate = backoffRate;
        this.maximumExpiryTimeInMS = maximumExpiryTimeInMS;
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.backOff = backOff;
    }

    @Override
    public void backOff(int attemptCount) {
        long sleepTime = this.calculateDelay(attemptCount);
        aqueductInstrumentation.logWarn("backing off for {} milliseconds ", sleepTime);
        aqueductInstrumentation.captureSleepTime(RETRY_SLEEP_TIME_MILLISECONDS, toIntExact(sleepTime));
        backOff.inMilliSeconds(sleepTime);
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
