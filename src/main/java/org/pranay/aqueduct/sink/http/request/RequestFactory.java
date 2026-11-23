package org.pranay.aqueduct.sink.http.request;


import org.pranay.aqueduct.config.HttpSinkConfig;
import org.pranay.aqueduct.config.enums.HttpSinkRequestMethodType;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.proto.ProtoToFieldMapper;
import org.pranay.aqueduct.serializer.MessageSerializer;
import org.pranay.aqueduct.sink.http.request.entity.RequestEntityBuilder;
import org.pranay.aqueduct.sink.http.request.header.HeaderBuilder;
import org.pranay.aqueduct.sink.http.request.types.ParameterizedHeaderRequest;
import org.pranay.aqueduct.sink.http.request.types.ParameterizedUriRequest;
import org.pranay.aqueduct.sink.http.request.types.Request;
import org.pranay.aqueduct.sink.http.request.types.SimpleRequest;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.http.factory.SerializerFactory;
import org.pranay.aqueduct.sink.http.request.body.JsonBody;
import org.pranay.aqueduct.sink.http.request.types.DynamicUrlRequest;
import org.pranay.aqueduct.sink.http.request.uri.UriBuilder;
import org.pranay.aqueduct.sink.http.request.uri.UriParser;
import org.pranay.stencil.client.StencilClient;
import org.pranay.stencil.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * Request factory create requests based on configuration.
 */
public class RequestFactory {

    private HttpSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;
    private AqueductInstrumentation aqueductInstrumentation;

    /**
     * Instantiates a new Request factory.
     *
     * @param statsDReporter the statsd reporter
     * @param httpSinkConfig the http sink config
     * @param stencilClient  the stencil client
     * @param uriParser      the uri parser
     */
    public RequestFactory(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        aqueductInstrumentation = new AqueductInstrumentation(this.statsDReporter, RequestFactory.class);
    }

    public Request createRequest() {
        JsonBody body = createBody();
        HttpSinkRequestMethodType httpSinkRequestMethodType = httpSinkConfig.getSinkHttpRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getSinkHttpHeaders());
        UriBuilder uriBuilder = new UriBuilder(httpSinkConfig.getSinkHttpServiceUrl(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        List<Request> requests = Arrays.asList(
                new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new DynamicUrlRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()),
                new ParameterizedUriRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()));

        Request request = requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType));
        aqueductInstrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        Parser protoParser = stencilClient.getParser(httpSinkConfig.getSinkHttpParameterSchemaProtoClass());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getInputSchemaProtoToColumnMapping());
    }

    private JsonBody createBody() {
        MessageSerializer messageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient,
                statsDReporter)
                .build();
        return new JsonBody(messageSerializer);
    }
}
