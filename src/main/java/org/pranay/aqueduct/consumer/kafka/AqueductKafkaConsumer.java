package org.pranay.aqueduct.consumer.kafka;

import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.message.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.pranay.aqueduct.metrics.Metrics.FAILURE_TAG;
import static org.pranay.aqueduct.metrics.Metrics.SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL;
import static org.pranay.aqueduct.metrics.Metrics.SUCCESS_TAG;

/**
 * A class responsible for consuming and committing kafka records.
 */
public class AqueductKafkaConsumer implements AutoCloseable {

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final KafkaConsumerConfig consumerConfig;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new ConcurrentHashMap<>();

    /**
     * A Constructor.
     *
     * @param kafkaConsumer           {@see KafkaConsumer}
     * @param config                  Consumer configuration.
     * @param aqueductInstrumentation Contain logging and metrics collection
     */
    public AqueductKafkaConsumer(Consumer<byte[], byte[]> kafkaConsumer, KafkaConsumerConfig config, AqueductInstrumentation aqueductInstrumentation) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerConfig = config;
        this.aqueductInstrumentation = aqueductInstrumentation;
    }

    /**
     * method to read next batch of messages from kafka.
     *
     * @return list of EsbMessage {@see EsbMessage}
     */
    public List<Message> readMessages() {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(consumerConfig.getSourceKafkaPollTimeoutMs()));
        aqueductInstrumentation.logInfo("Pulled {} messages", records.count());
        aqueductInstrumentation.capturePulledMessageHistogram(records.count());
        aqueductInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.CONSUMER, records.count());
        List<Message> messages = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp(), System.currentTimeMillis()));
            aqueductInstrumentation.logDebug("Pulled record: {}", record);
        }
        return messages;
    }

    public void close() {
        try {
            aqueductInstrumentation.logInfo("Consumer is closing");
            this.kafkaConsumer.close();
        } catch (Exception e) {
            aqueductInstrumentation.captureNonFatalError("aqueduct_error_event", e, "Exception while closing consumer");
        }
    }

    public void commit() {
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            kafkaConsumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    aqueductInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
                } else {
                    aqueductInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
                }
            });
        } else {
            kafkaConsumer.commitSync();
        }
    }

    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> latestOffsets =
                offsets.entrySet()
                        .stream()
                        .filter(metadataEntry -> !committedOffsets.containsKey(metadataEntry.getKey())
                                || metadataEntry.getValue().offset() > committedOffsets.get(metadataEntry.getKey()).offset())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (latestOffsets.isEmpty()) {
            return;
        }
        latestOffsets.forEach((k, v) ->
                aqueductInstrumentation.logInfo("Committing Offsets " + k.topic() + ":" + k.partition() + "=>" + v.offset()));
        if (consumerConfig.isSourceKafkaAsyncCommitEnable()) {
            commitAsync(latestOffsets);
        } else {
            kafkaConsumer.commitSync(latestOffsets);
        }
        committedOffsets.putAll(latestOffsets);
    }

    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitAsync(offsets, this::onComplete);
    }

    private void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            aqueductInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, FAILURE_TAG);
        } else {
            aqueductInstrumentation.incrementCounter(SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL, SUCCESS_TAG);
        }
    }
}
