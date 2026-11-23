package org.pranay.aqueduct.sink.grpc;



import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.AbstractSink;
import org.pranay.aqueduct.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import org.pranay.stencil.client.StencilClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see org.pranay.aqueduct.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    private final GrpcClient grpcClient;
    private List<Message> messages;
    private StencilClient stencilClient;

    public GrpcSink(AqueductInstrumentation aqueductInstrumentation, GrpcClient grpcClient, StencilClient stencilClient) {
        super(aqueductInstrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> failedMessages = new ArrayList<>();

        for (Message message : this.messages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            getAqueductInstrumentation().logDebug("Response: {}", response);
            Object m = response.getField(response.getDescriptorForType().findFieldByName("success"));
            boolean success = (m != null) ? Boolean.valueOf(String.valueOf(m)) : false;

            if (!success) {
                getAqueductInstrumentation().logWarn("Grpc Service returned error");
                failedMessages.add(message);
            }
        }
        getAqueductInstrumentation().logDebug("Failed messages count: {}", failedMessages.size());
        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messages2) throws DeserializerException {
        this.messages = messages2;
    }

    @Override
    public void close() throws IOException {
        getAqueductInstrumentation().logInfo("GRPC connection closing");
        this.messages = new ArrayList<>();
        stencilClient.close();
    }
}
