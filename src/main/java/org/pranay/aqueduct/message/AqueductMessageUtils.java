package org.pranay.aqueduct.message;

import org.pranay.depot.common.Tuple;

import java.util.List;
import java.util.stream.Collectors;

public class AqueductMessageUtils {

    public static List<org.pranay.depot.message.Message> convertToDepotMessage(List<Message> messages) {
        return messages.stream().map(message ->
                        new org.pranay.depot.message.Message(
                                message.getLogKey(),
                                message.getLogMessage(),
                                new Tuple<>("message_topic", message.getTopic()),
                                new Tuple<>("message_partition", message.getPartition()),
                                new Tuple<>("message_offset", message.getOffset()),
                                new Tuple<>("message_headers", message.getHeaders()),
                                new Tuple<>("message_timestamp", message.getTimestamp()),
                                new Tuple<>("load_time", message.getConsumeTimestamp())))
                .collect(Collectors.toList());
    }
}
