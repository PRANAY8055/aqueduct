package org.pranay.aqueduct.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pranay.aqueduct.config.BlobSinkConfig;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.exception.EmptyMessageException;
import org.pranay.aqueduct.exception.UnknownFieldsException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.proto.ProtoUtils;
import org.pranay.aqueduct.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import org.pranay.stencil.client.StencilClient;
import org.pranay.stencil.Parser;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MessageDeSerializer {

    private final Descriptors.FileDescriptor kafkaMetadataFileDescriptor;
    private final Parser protoParser;
    private final BlobSinkConfig sinkConfig;

    public MessageDeSerializer(BlobSinkConfig sinkConfig, StencilClient stencilClient) {
        this.sinkConfig = sinkConfig;
        this.protoParser = stencilClient.getParser(sinkConfig.getInputSchemaProtoClass());
        this.kafkaMetadataFileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getOutputKafkaMetadataColumnName());
    }

    public Record deSerialize(Message message) throws DeserializerException {
        try {
            if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                throw new EmptyMessageException();
            }
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());

            if (!sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
                throw new UnknownFieldsException(dynamicMessage);
            }

            DynamicMessage kafkaMetadata = KafkaMetadataUtils.createKafkaMetadata(kafkaMetadataFileDescriptor, message, sinkConfig.getOutputKafkaMetadataColumnName());
            return new Record(dynamicMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }
}
