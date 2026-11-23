package org.pranay.aqueduct.launch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.pranay.aqueduct.metrics.AqueductInstrumentation;

/**
 * The Task with parallelism.
 */
public class Task {

    private final ExecutorService executorService;
    private int parallelism;
    private int threadCleanupDelay;
    private Consumer<Runnable> task;
    private Runnable taskFinishCallback;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;
    private AqueductInstrumentation aqueductInstrumentation;

    /**
     * Instantiates a new Task.
     *
     * @param parallelism             the parallelism
     * @param threadCleanupDelay      the thread cleanup delay
     * @param aqueductInstrumentation the instrumentation
     * @param task                    the task
     */
    public Task(int parallelism, int threadCleanupDelay, AqueductInstrumentation aqueductInstrumentation, Consumer<Runnable> task) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.task = task;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        taskFinishCallback = countDownLatch::countDown;
        this.aqueductInstrumentation = aqueductInstrumentation;
    }

    public Task run() {
        for (int i = 0; i < parallelism; i++) {
            fnFutures.add(executorService.submit(() -> {
                task.accept(taskFinishCallback);
            }));
        }
        return this;
    }

    public void waitForCompletion() throws InterruptedException {
        aqueductInstrumentation.logInfo("waiting for completion");
        countDownLatch.await();
    }

    public Task stop() {
        try {
            aqueductInstrumentation.logInfo("Stopping task thread");
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            aqueductInstrumentation.logInfo("Sleeping thread during clean up for {} duration", threadCleanupDelay);
            Thread.sleep(threadCleanupDelay);
        } catch (InterruptedException e) {
            aqueductInstrumentation.captureNonFatalError("aqueduct_error_event", e, "error stopping tasks");
        }
        return this;
    }
}
