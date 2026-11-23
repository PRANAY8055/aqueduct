package org.pranay.aqueduct.utils;

import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;

/**
 * A callback to log when the partition rebalancing happens.
 */
@AllArgsConstructor
public class ConsumerRebalancer implements ConsumerRebalanceListener {

    private AqueductInstrumentation aqueductInstrumentation;

    /**
     * Function to run On partitions revoked.
     *
     * @param partitions list of partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        aqueductInstrumentation.logWarn("Partitions Revoked {}", Arrays.toString(partitions.toArray()));
    }

    /**
     * Function to run On partitions assigned.
     *
     * @param partitions list of partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        aqueductInstrumentation.logInfo("Partitions Assigned {}", Arrays.toString(partitions.toArray()));
    }
}
