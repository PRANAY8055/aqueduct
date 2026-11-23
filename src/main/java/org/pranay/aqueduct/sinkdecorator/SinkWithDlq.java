package org.pranay.aqueduct.sinkdecorator;

import org.pranay.aqueduct.config.DlqConfig;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.error.ErrorHandler;
import org.pranay.aqueduct.error.ErrorScope;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.aqueduct.sink.dlq.DlqWriter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.pranay.aqueduct.metrics.Metrics.DLQ_MESSAGES_TOTAL;
import static org.pranay.aqueduct.metrics.Metrics.DLQ_RETRY_ATTEMPTS_TOTAL;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    public static final String DLQ_BATCH_KEY = "dlq-batch-key";
    private final DlqWriter writer;
    private final BackOffProvider backOffProvider;
    private final DlqConfig dlqConfig;
    private final ErrorHandler errorHandler;

    private final AqueductInstrumentation aqueductInstrumentation;

    public SinkWithDlq(Sink sink, DlqWriter writer, BackOffProvider backOffProvider, DlqConfig dlqConfig, ErrorHandler errorHandler, AqueductInstrumentation aqueductInstrumentation) {
        super(sink);
        this.writer = writer;
        this.backOffProvider = backOffProvider;
        this.errorHandler = errorHandler;
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.dlqConfig = dlqConfig;
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param inputMessages list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        if (messages.isEmpty()) {
            return messages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(messages, ErrorScope.DLQ);
        List<Message> returnedMessages = doDLQ(splitLists.get(Boolean.TRUE));
        if (!returnedMessages.isEmpty() && dlqConfig.getDlqRetryFailAfterMaxAttemptEnable()) {
            throw new IOException("exhausted maximum number of allowed retry attempts to write messages to DLQ");
        }
        if (super.canManageOffsets()) {
            super.addOffsetsAndSetCommittable(messages);
        }
        returnedMessages.addAll(splitLists.get(Boolean.FALSE));
        return returnedMessages;
    }

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    private List<Message> doDLQ(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = new LinkedList<>(messages);
        retryQueueMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            aqueductInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });
        int attemptCount = 1;
        while (attemptCount <= this.dlqConfig.getDlqRetryMaxAttempts() && !retryQueueMessages.isEmpty()) {
            aqueductInstrumentation.incrementCounter(DLQ_RETRY_ATTEMPTS_TOTAL);
            retryQueueMessages = writer.write(retryQueueMessages);
            retryQueueMessages.forEach(message -> Optional.ofNullable(message.getErrorInfo())
                    .flatMap(errorInfo -> Optional.ofNullable(errorInfo.getException()))
                    .ifPresent(e -> aqueductInstrumentation.captureDLQErrors(message, e)));
            backOff(retryQueueMessages, attemptCount);
            attemptCount++;
        }
        if (!retryQueueMessages.isEmpty()) {
            aqueductInstrumentation.logInfo("failed to be processed by DLQ messages: {}", retryQueueMessages.size());
        }
        aqueductInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryQueueMessages.size());
        retryQueueMessages.forEach(m -> aqueductInstrumentation.captureMessageMetrics(DLQ_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        aqueductInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.DLQ, messages.size() - retryQueueMessages.size());
        return retryQueueMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
