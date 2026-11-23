package org.pranay.aqueduct.sinkdecorator;

import com.google.protobuf.DynamicMessage;
import org.pranay.aqueduct.config.AppConfig;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.error.ErrorHandler;
import org.pranay.aqueduct.error.ErrorScope;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.aqueduct.sink.common.KeyOrMessageParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.pranay.aqueduct.metrics.Metrics.RETRY_MESSAGES_TOTAL;
import static org.pranay.aqueduct.metrics.Metrics.RETRY_ATTEMPTS_TOTAL;

/**
 * Pushes messages with configured retry.
 */
public class SinkWithRetry extends SinkDecorator {

    private final BackOffProvider backOffProvider;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final AppConfig appConfig;
    private final KeyOrMessageParser parser;
    private final ErrorHandler errorHandler;

    public SinkWithRetry(Sink sink, BackOffProvider backOffProvider, AqueductInstrumentation aqueductInstrumentation, AppConfig appConfig, KeyOrMessageParser parser, ErrorHandler errorHandler) {
        super(sink);
        this.backOffProvider = backOffProvider;
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.appConfig = appConfig;
        this.parser = parser;
        this.errorHandler = errorHandler;
    }

    /**
     * Pushes messages with retry.
     *
     * @param inputMessages list of messages
     * @return the remaining failed messages
     * @throws IOException           the io exception
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.isEmpty()) {
            return failedMessages;
        }
        Map<Boolean, List<Message>> splitLists = errorHandler.split(failedMessages, ErrorScope.RETRY);
        List<Message> messagesAfterRetry = doRetry(splitLists.get(Boolean.TRUE));
        if (!messagesAfterRetry.isEmpty() && appConfig.getRetryFailAfterMaxAttemptsEnable()) {
            throw new IOException("exceeded maximum Sink retry attempts");
        }
        messagesAfterRetry.addAll(splitLists.get(Boolean.FALSE));
        return messagesAfterRetry;
    }

    private void logDebug(List<Message> messageList) throws IOException {
        if (aqueductInstrumentation.isDebugEnabled()) {
            switch (appConfig.getInputSchemaType()) {
                case PROTOBUF:
                    List<DynamicMessage> serializedBody = new ArrayList<>();
                    for (Message message : messageList) {
                        serializedBody.add(parser.parse(message));
                    }
                    aqueductInstrumentation.logDebug("Retry failed messages: \n{}", serializedBody.toString());
                    break;
                case JSON:
                    List<String> messages = messageList.stream().map(m -> {
                        if (appConfig.getKafkaRecordParserMode().equals("key")) {
                            return new String(m.getLogKey());
                        } else {
                            return new String(m.getLogMessage());
                        }
                    }).collect(Collectors.toList());
                    aqueductInstrumentation.logDebug("Retry failed messages: \n{}", messages.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected value: " + appConfig.getInputSchemaType());
            }
        }
    }

    private void backOff(List<Message> messageList, int attemptCount) {
        if (messageList.isEmpty()) {
            return;
        }
        backOffProvider.backOff(attemptCount);
    }

    private List<Message> doRetry(List<Message> messages) throws IOException {
        List<Message> retryMessages = new LinkedList<>(messages);
        aqueductInstrumentation.logInfo("Maximum retry attempts: {}", appConfig.getRetryMaxAttempts());
        retryMessages.forEach(m -> {
            m.setDefaultErrorIfNotPresent();
            aqueductInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, m.getErrorInfo().getErrorType(), 1);
        });

        int attemptCount = 1;
        while ((attemptCount <= appConfig.getRetryMaxAttempts() && !retryMessages.isEmpty())
                || (appConfig.getRetryMaxAttempts() == Integer.MAX_VALUE && !retryMessages.isEmpty())) {
            aqueductInstrumentation.incrementCounter(RETRY_ATTEMPTS_TOTAL);
            aqueductInstrumentation.logInfo("Retrying messages attempt count: {}, Number of messages: {}", attemptCount, messages.size());
            logDebug(retryMessages);
            retryMessages = super.pushMessage(retryMessages);
            backOff(retryMessages, attemptCount);
            attemptCount++;
        }
        aqueductInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - retryMessages.size());
        retryMessages.forEach(m -> aqueductInstrumentation.captureMessageMetrics(RETRY_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1));
        return retryMessages;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
