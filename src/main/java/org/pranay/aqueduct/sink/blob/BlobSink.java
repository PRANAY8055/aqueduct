package org.pranay.aqueduct.sink.blob;

import org.pranay.aqueduct.consumer.kafka.OffsetManager;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.exception.EmptyMessageException;
import org.pranay.aqueduct.exception.SinkException;
import org.pranay.aqueduct.exception.UnknownFieldsException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.blob.message.MessageDeSerializer;
import org.pranay.aqueduct.sink.blob.message.Record;
import org.pranay.aqueduct.sink.blob.writer.WriterOrchestrator;
import org.pranay.depot.error.ErrorInfo;
import org.pranay.depot.error.ErrorType;
import org.pranay.aqueduct.sink.AbstractSink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BlobSink extends AbstractSink {

    private final WriterOrchestrator writerOrchestrator;
    private final OffsetManager offsetManager;
    private final MessageDeSerializer messageDeSerializer;

    private List<Message> messages;

    public BlobSink(AqueductInstrumentation aqueductInstrumentation, String sinkType, OffsetManager offsetManager, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(aqueductInstrumentation, sinkType);
        this.offsetManager = offsetManager;
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<Message> failedMessages = new LinkedList<>();
        Map<Object, List<Message>> fileToMessages = new HashMap<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                String filePath = writerOrchestrator.write(record);
                fileToMessages.computeIfAbsent(filePath, key -> new ArrayList<>()).add(message);
            } catch (EmptyMessageException e) {
                getAqueductInstrumentation().logWarn("empty message found on topic: {}, partition: {}, offset: {}",
                        message.getTopic(), message.getPartition(), message.getOffset());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                failedMessages.add(message);
            } catch (UnknownFieldsException e) {
                getAqueductInstrumentation().logWarn(e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                failedMessages.add(message);
            } catch (DeserializerException e) {
                getAqueductInstrumentation().logWarn("message deserialization failed on topic: {}, partition: {}, offset: {}, reason: {}",
                        message.getTopic(), message.getPartition(), message.getOffset(), e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                failedMessages.add(message);
            } catch (Exception e) {
                throw new SinkException("Failed to deserialize the message", e);
            }
        }
        offsetManager.addOffsetToBatch(fileToMessages);
        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messageList) throws IOException, SQLException {
        this.messages = messageList;
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    @Override
    public void calculateCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
    }

    @Override
    public boolean canManageOffsets() {
        return true;
    }

    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        offsetManager.addOffsetsAndSetCommittable(messageList);
    }
}
