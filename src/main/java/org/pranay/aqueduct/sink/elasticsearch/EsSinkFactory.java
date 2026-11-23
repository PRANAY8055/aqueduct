package org.pranay.aqueduct.sink.elasticsearch;


import org.pranay.aqueduct.config.EsSinkConfig;
import org.pranay.aqueduct.config.enums.SinkType;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.serializer.MessageToJson;
import org.pranay.aqueduct.sink.elasticsearch.request.EsRequestHandler;
import org.pranay.aqueduct.sink.elasticsearch.request.EsRequestHandlerFactory;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sink factory to configuration and create Elastic search sink.
 */
public class EsSinkFactory {

    /**
     * Creates Elastic search sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return created sink
     */
    public static Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        EsSinkConfig esSinkConfig = ConfigFactory.create(EsSinkConfig.class, configuration);

        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, EsSinkFactory.class);
        String esConfig = String.format("\n\tES connection urls: %s\n\tES index name: %s\n\tES id field: %s\n\tES message type: %s"
                        + "\n\tES type name: %s\n\tES request timeout in ms: %s\n\tES retry status code blacklist: %s"
                        + "\n\tES routing key name: %s\n\tES wait for active shards count: %s\n\tES update only mode: %s"
                        + "\n\tES should preserve proto filed names: %s",
                esSinkConfig.getSinkEsConnectionUrls(), esSinkConfig.getSinkEsIndexName(), esSinkConfig.getSinkEsIdField(), esSinkConfig.getSinkEsInputMessageType(),
                esSinkConfig.getSinkEsTypeName(), esSinkConfig.getSinkEsRequestTimeoutMs(), esSinkConfig.getSinkEsRetryStatusCodeBlacklist(),
                esSinkConfig.getSinkEsRoutingKeyName(), esSinkConfig.getSinkEsShardsActiveWaitCount(), esSinkConfig.isSinkEsModeUpdateOnlyEnable(),
                esSinkConfig.isSinkEsPreserveProtoFieldNamesEnable());
        aqueductInstrumentation.logDebug(esConfig);
        EsRequestHandler esRequestHandler = new EsRequestHandlerFactory(esSinkConfig, new AqueductInstrumentation(statsDReporter, EsRequestHandlerFactory.class),
                esSinkConfig.getSinkEsIdField(), esSinkConfig.getSinkEsInputMessageType(),
                new MessageToJson(stencilClient.getParser(esSinkConfig.getInputSchemaProtoClass()), esSinkConfig.isSinkEsPreserveProtoFieldNamesEnable(), false),
                esSinkConfig.getSinkEsTypeName(),
                esSinkConfig.getSinkEsIndexName(),
                esSinkConfig.getSinkEsRoutingKeyName())
                .getRequestHandler();

        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getSinkEsConnectionUrls(), aqueductInstrumentation);
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts));
        aqueductInstrumentation.logInfo("ES connection established");
        return new EsSink(new AqueductInstrumentation(statsDReporter, EsSink.class), SinkType.ELASTICSEARCH.name().toLowerCase(), client, esRequestHandler,
                esSinkConfig.getSinkEsRequestTimeoutMs(), esSinkConfig.getSinkEsShardsActiveWaitCount(), getStatusCodesAsList(esSinkConfig.getSinkEsRetryStatusCodeBlacklist()));
    }

    protected static HttpHost[] getHttpHosts(String esConnectionUrls, AqueductInstrumentation aqueductInstrumentation) {
        if (esConnectionUrls != null && !esConnectionUrls.isEmpty()) {
            String[] esNodes = esConnectionUrls.trim().split(",");
            HttpHost[] httpHosts = new HttpHost[esNodes.length];
            for (int i = 0; i < esNodes.length; i++) {
                String[] node = esNodes[i].trim().split(":");
                if (node.length <= 1) {
                    throw new IllegalArgumentException("SINK_ES_CONNECTION_URLS should contain host and port both");
                }
                httpHosts[i] = new HttpHost(node[0].trim(), Integer.parseInt(node[1].trim()));
            }
            return httpHosts;
        } else {
            aqueductInstrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("SINK_ES_CONNECTION_URLS is empty or null");
        }
    }

    protected static List<String> getStatusCodesAsList(String esRetryStatusCodeBlacklist) {
        return Arrays
                .stream(esRetryStatusCodeBlacklist.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
