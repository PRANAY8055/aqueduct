package org.pranay.aqueduct.sink;

import org.pranay.aqueduct.exception.ConfigurationException;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.exception.SinkException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * Abstract sink.
 * All other type of sink will implement this.
 */
@AllArgsConstructor
public abstract class AbstractSink implements Closeable, Sink {

    private final AqueductInstrumentation aqueductInstrumentation;
    private final String sinkType;

    /**
     * Method to push messages to sink.
     *
     * @param messages the messages
     * @return the list of failed messages
     * @throws DeserializerException when invalid kafka message is encountered
     */
    public List<Message> pushMessage(List<Message> messages) {
        List<Message> failedMessages = messages;
        Instant executionStartTime = null;
        try {
            aqueductInstrumentation.logInfo("Preparing {} messages", messages.size());
            aqueductInstrumentation.captureMessageBatchSize(messages.size());
            aqueductInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, messages.size());
            prepare(messages);
            aqueductInstrumentation.capturePreExecutionLatencies(messages);
            executionStartTime = aqueductInstrumentation.startExecution();
            failedMessages = execute();
            aqueductInstrumentation.logInfo("Pushed {} messages", messages.size() - failedMessages.size());
        } catch (DeserializerException | ConfigurationException | NullPointerException | SinkException e) {
            throw e;
        } catch (Exception e) {
            if (!messages.isEmpty()) {
                aqueductInstrumentation.logWarn("Failed to push {} messages to sink", messages.size());
            }
            aqueductInstrumentation.captureNonFatalError("aqueduct_error_event", e, "caught {} {}", e.getClass(), e.getMessage());
            failedMessages = messages;
        } finally {
            // Process success,failure and error metrics
            if (executionStartTime != null) {
                aqueductInstrumentation.captureSinkExecutionTelemetry(sinkType, messages.size());
            }
            aqueductInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - failedMessages.size());
            aqueductInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.SINK, messages.size() - failedMessages.size());
            processFailedMessages(failedMessages);
        }
        return failedMessages;
    }

    private void processFailedMessages(List<Message> failedMessages) {
        if (failedMessages.size() > 0) {
            aqueductInstrumentation.logError("Failed to Push {} messages to sink ", failedMessages.size());
            failedMessages.forEach(m -> {
                m.setDefaultErrorIfNotPresent();
                aqueductInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1);
                aqueductInstrumentation.captureErrorMetrics(m.getErrorInfo().getErrorType());
                aqueductInstrumentation.logError("Failed to Push message. Error: {},Topic: {}, Partition: {},Offset: {}",
                        m.getErrorInfo().getErrorType(),
                        m.getTopic(),
                        m.getPartition(),
                        m.getOffset());
            });
        }
    }

    /**
     * Gets instrumentation.
     *
     * @return the instrumentation
     */
    public AqueductInstrumentation getAqueductInstrumentation() {
        return aqueductInstrumentation;
    }

    /**
     * send messages to the sink.
     *
     * @return the list
     * @throws Exception the exception
     */
    protected abstract List<Message> execute() throws Exception;

    /**
     * process the messages before sending to the sink.
     *
     * @param messages the messages
     * @throws DeserializerException the deserializer exception
     * @throws IOException           the io exception
     * @throws SQLException          the sql exception
     */
    protected abstract void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException;
}
