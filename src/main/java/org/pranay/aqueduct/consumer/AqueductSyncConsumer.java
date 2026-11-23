package org.pranay.aqueduct.consumer;

import org.pranay.aqueduct.consumer.kafka.ConsumerAndOffsetManager;
import org.pranay.aqueduct.exception.AqueductConsumerFailedException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.filter.FilterException;
import org.pranay.aqueduct.filter.FilteredMessages;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.aqueduct.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.pranay.aqueduct.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

/**
 * Aqueduct consumer reads messages from Generic consumer and pushes messages to the configured sink.
 */
@AllArgsConstructor
public class AqueductSyncConsumer implements AqueductConsumer {

    private final Sink sink;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final AqueductFilter aqueductFilter;
    private final AqueductInstrumentation aqueductInstrumentation;

    @Override
    public void process() throws IOException {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = aqueductFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                sink.pushMessage(filteredMessages.getValidMessages());
                consumerAndOffsetManager.addOffsetsAndSetCommittable(filteredMessages.getValidMessages());
            }
            consumerAndOffsetManager.commit();
            aqueductInstrumentation.logInfo("Processed {} records in consumer", messages.size());
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new AqueductConsumerFailedException(e);
        } finally {
            aqueductInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    @Override
    public void close() throws IOException {
        sink.close();
        tracer.close();
        consumerAndOffsetManager.close();
        aqueductInstrumentation.close();
    }
}
