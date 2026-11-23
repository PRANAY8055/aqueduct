package org.pranay.aqueduct.sink;

import org.pranay.depot.Sink;
import org.pranay.depot.SinkResponse;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.message.AqueductMessageUtils;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericSink extends AbstractSink {
    private final List<Message> messageList = new ArrayList<>();
    private final Sink sink;

    public GenericSink(AqueductInstrumentation aqueductInstrumentation, String sinkType, Sink sink) {
        super(aqueductInstrumentation, sinkType);
        this.sink = sink;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<org.pranay.depot.message.Message> messages = AqueductMessageUtils.convertToDepotMessage(messageList);
        SinkResponse response = sink.pushToSink(messages);
        return response.getErrors().keySet().stream()
                .map(index -> {
                    Message message = messageList.get(index.intValue());
                    message.setErrorInfo(response.getErrorsFor(index));
                    return message;
                }).collect(Collectors.toList());
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        messageList.clear();
        messageList.addAll(messages);
    }

    @Override
    public void close() throws IOException {
        sink.close();
    }
}
