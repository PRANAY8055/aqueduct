package org.pranay.aqueduct.sinkdecorator;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.Sink;

import java.io.IOException;
import java.util.List;

public class SinkFinal extends SinkDecorator {
    private final AqueductInstrumentation aqueductInstrumentation;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink wrapped sink object
     */

    public SinkFinal(Sink sink, AqueductInstrumentation aqueductInstrumentation) {
        super(sink);
        this.aqueductInstrumentation = aqueductInstrumentation;
    }

    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> failedMessages = super.pushMessage(inputMessages);
        if (failedMessages.size() > 0) {
            aqueductInstrumentation.logInfo("Ignoring messages {}", failedMessages.size());
            aqueductInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, failedMessages.size());
        }
        return failedMessages;
    }
}
