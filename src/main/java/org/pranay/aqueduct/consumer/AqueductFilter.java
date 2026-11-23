package org.pranay.aqueduct.consumer;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.filter.Filter;
import org.pranay.aqueduct.filter.FilterException;
import org.pranay.aqueduct.filter.FilteredMessages;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class AqueductFilter {
    private final Filter filter;
    private final AqueductInstrumentation aqueductInstrumentation;

    public FilteredMessages applyFilter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessage = filter.filter(messages);
        int filteredMessageCount = filteredMessage.sizeOfInvalidMessages();
        if (filteredMessageCount > 0) {
            aqueductInstrumentation.captureFilteredMessageCount(filteredMessageCount);
            aqueductInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.FILTERED, filteredMessageCount);
        }
        return filteredMessage;
    }
}
