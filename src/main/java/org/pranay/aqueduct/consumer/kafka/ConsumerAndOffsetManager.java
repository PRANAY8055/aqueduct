package org.pranay.aqueduct.consumer.kafka;

import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * This class has APIs to read from kafka and also provide offset management.
 * There are 2 use cases for this class.
 * 1. AqueductConsumer:
 * consumerOffsetManager.readMessagesFromKafka(); // Read messages from kafka.
 * consumerOffsetManager.addOffsetsAndSetCommittable(messages); // add offsets for messages.
 * consumerOffsetManager.commit(); // commit all committable offsets for all partitions.
 * <p>
 * 2. AqueductAsyncConsumer:
 * consumerOffsetManager.readMessagesFromKafka();
 * consumerOffsetManager.addOffsets(key, messages);
 * consumerOffsetManager.setCommittable(key);
 * consumerOffsetManager.commit();
 * <p>
 * <p>
 * OffsetManager is shared between consumer and the sink.
 * So the offsets added there will be available here to commit.
 * <p>
 * consumerOffsetManager.commit() calls the sink method to calculate committable offsets.
 * then it fetches the offsets from offsetManager.getCommittableOffsets() and uses kafka api to commit.
 */
public class ConsumerAndOffsetManager implements AutoCloseable {
    private final OffsetManager offsetManager;
    private final List<Sink> sinks;
    private final AqueductKafkaConsumer aqueductKafkaConsumer;
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final boolean canSinkManageOffsets;
    private long lastCommitTimeStamp = 0;

    public ConsumerAndOffsetManager(
            List<Sink> sinks,
            OffsetManager offsetManager,
            AqueductKafkaConsumer aqueductKafkaConsumer,
            KafkaConsumerConfig kafkaConsumerConfig,
            AqueductInstrumentation aqueductInstrumentation) {
        this.sinks = sinks;
        this.offsetManager = offsetManager;
        this.aqueductKafkaConsumer = aqueductKafkaConsumer;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.canSinkManageOffsets = sinks.get(0).canManageOffsets();
    }

    public void addOffsets(Object key, List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetToBatch(key, messages);
        }
    }

    public void setCommittable(Object key) {
        if (!canSinkManageOffsets) {
            offsetManager.setCommittable(key);
        }
    }

    public void addOffsetsAndSetCommittable(List<Message> messages) {
        if (!canSinkManageOffsets) {
            offsetManager.addOffsetsAndSetCommittable(messages);
        }
    }

    /**
     * Force-Update the offsets into offset manager regardless of sink managing the offsets.
     *
     * @param messages list of messages set to be committable
     */
    public void forceAddOffsetsAndSetCommittable(List<Message> messages) {
        offsetManager.addOffsetsAndSetCommittable(messages);
    }

    public List<Message> readMessages() {
        return aqueductKafkaConsumer.readMessages();
    }

    public void commit() {
        long currentTimeStamp = System.currentTimeMillis();
        if (currentTimeStamp - lastCommitTimeStamp > kafkaConsumerConfig.getSourceKafkaConsumerManualCommitMinIntervalMs()) {
            if (kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable()) {
                sinks.forEach(Sink::calculateCommittableOffsets);
                aqueductKafkaConsumer.commit(offsetManager.getCommittableOffset());
            } else {
                aqueductKafkaConsumer.commit();
            }
            lastCommitTimeStamp = currentTimeStamp;
        }
    }

    @Override
    public void close() throws IOException {
        if (aqueductKafkaConsumer != null) {
            aqueductInstrumentation.logInfo("closing consumer");
            aqueductKafkaConsumer.close();
        }
    }

}
