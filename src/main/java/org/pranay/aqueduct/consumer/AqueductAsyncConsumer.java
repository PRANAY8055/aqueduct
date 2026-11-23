package org.pranay.aqueduct.consumer;

import org.pranay.aqueduct.consumer.kafka.ConsumerAndOffsetManager;
import org.pranay.aqueduct.exception.AqueductConsumerFailedException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.SinkPool;
import org.pranay.aqueduct.filter.FilterException;
import org.pranay.aqueduct.filter.FilteredMessages;
import org.pranay.aqueduct.tracer.SinkTracer;
import io.opentracing.Span;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static org.pranay.aqueduct.metrics.Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS;

@AllArgsConstructor
public class AqueductAsyncConsumer implements AqueductConsumer {
    private final SinkPool sinkPool;
    private final SinkTracer tracer;
    private final ConsumerAndOffsetManager consumerAndOffsetManager;
    private final AqueductFilter aqueductFilter;
    private final AqueductInstrumentation aqueductInstrumentation;

    @Override
    public void process() {
        Instant beforeCall = Instant.now();
        try {
            List<Message> messages = consumerAndOffsetManager.readMessages();
            List<Span> spans = tracer.startTrace(messages);
            FilteredMessages filteredMessages = aqueductFilter.applyFilter(messages);
            if (filteredMessages.sizeOfInvalidMessages() > 0) {
                consumerAndOffsetManager.forceAddOffsetsAndSetCommittable(filteredMessages.getInvalidMessages());
            }
            if (filteredMessages.sizeOfValidMessages() > 0) {
                List<Message> validMessages = filteredMessages.getValidMessages();
                Future<List<Message>> scheduledTask = scheduleTask(validMessages);
                consumerAndOffsetManager.addOffsets(scheduledTask, validMessages);
            }
            sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            consumerAndOffsetManager.commit();
            tracer.finishTrace(spans);
        } catch (FilterException e) {
            throw new AqueductConsumerFailedException(e);
        } finally {
            aqueductInstrumentation.captureDurationSince(SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS, beforeCall);
        }
    }

    private Future<List<Message>> scheduleTask(List<Message> messages) {
        while (true) {
            Future<List<Message>> scheduledTask = sinkPool.submitTask(messages);
            if (scheduledTask == null) {
                aqueductInstrumentation.logInfo("The Queue is full");
                sinkPool.fetchFinishedSinkTasks().forEach(consumerAndOffsetManager::setCommittable);
            } else {
                aqueductInstrumentation.logInfo("Adding sink task");
                return scheduledTask;
            }
        }
    }

    @Override
    public void close() throws IOException {
        sinkPool.close();
        consumerAndOffsetManager.close();
        tracer.close();
        aqueductInstrumentation.close();
    }
}
