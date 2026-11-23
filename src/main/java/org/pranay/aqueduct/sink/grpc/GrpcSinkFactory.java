package org.pranay.aqueduct.sink.grpc;


import org.pranay.aqueduct.config.GrpcSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.grpc.client.GrpcClient;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.AbstractSink;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.pranay.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */
public class GrpcSinkFactory {

    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        aqueductInstrumentation.logDebug(grpcSinkConfig);

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort()).usePlaintext().build();

        GrpcClient grpcClient = new GrpcClient(new AqueductInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        aqueductInstrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new AqueductInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

}
