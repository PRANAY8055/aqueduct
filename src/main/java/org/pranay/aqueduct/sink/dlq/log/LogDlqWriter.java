package org.pranay.aqueduct.sink.dlq.log;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.depot.error.ErrorInfo;
import org.pranay.aqueduct.sink.dlq.DlqWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LogDlqWriter implements DlqWriter {
    private final AqueductInstrumentation aqueductInstrumentation;

    public LogDlqWriter(AqueductInstrumentation aqueductInstrumentation) {
        this.aqueductInstrumentation = aqueductInstrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        for (Message message : messages) {
            String key = message.getLogKey() == null ? "" : new String(message.getLogKey());
            String value = message.getLogMessage() == null ? "" : new String(message.getLogMessage());

            String error = "";
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo != null) {
                if (errorInfo.getException() != null) {
                    error = ExceptionUtils.getStackTrace(errorInfo.getException());
                }
            }

            aqueductInstrumentation.logInfo("key: {}\nvalue: {}\nerror: {}", key, value, error);
        }
        return new LinkedList<>();
    }

}
