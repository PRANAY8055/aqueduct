package org.pranay.aqueduct.sink.http;


import org.pranay.aqueduct.config.HttpSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.http.auth.OAuth2Credential;
import org.pranay.aqueduct.sink.http.request.RequestFactory;
import org.pranay.aqueduct.sink.http.request.types.Request;
import org.pranay.aqueduct.sink.http.request.uri.UriParser;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.AbstractSink;
import org.pranay.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

/**
 * Factory class to create the HTTP Sink.
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the HTTPSink sink implementation. {@see ParameterizedHTTPSinkConfig}
 */
public class HttpSinkFactory {

    /**
     * Create Http sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the statsd reporter
     * @param stencilClient  the stencil client
     * @return the http sink
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        HttpSinkConfig httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, HttpSinkFactory.class);

        CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig, statsDReporter);
        aqueductInstrumentation.logInfo("HTTP connection established");

        UriParser uriParser = new UriParser(stencilClient.getParser(httpSinkConfig.getInputSchemaProtoClass()), httpSinkConfig.getKafkaRecordParserMode());

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        return new HttpSink(new AqueductInstrumentation(statsDReporter, HttpSink.class), request, closeableHttpClient, stencilClient, httpSinkConfig.getSinkHttpRetryStatusCodeRanges(), httpSinkConfig.getSinkHttpRequestLogStatusCodeRanges());
    }

    private static CloseableHttpClient newHttpClient(HttpSinkConfig httpSinkConfig, StatsDReporter statsDReporter) {
        Integer maxHttpConnections = httpSinkConfig.getSinkHttpMaxConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs())
                .setConnectionRequestTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs())
                .setConnectTimeout(httpSinkConfig.getSinkHttpRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        if (httpSinkConfig.isSinkHttpOAuth2Enable()) {
            OAuth2Credential oauth2 = new OAuth2Credential(
                    new AqueductInstrumentation(statsDReporter, OAuth2Credential.class),
                    httpSinkConfig.getSinkHttpOAuth2ClientName(),
                    httpSinkConfig.getSinkHttpOAuth2ClientSecret(),
                    httpSinkConfig.getSinkHttpOAuth2Scope(),
                    httpSinkConfig.getSinkHttpOAuth2AccessTokenUrl());
            builder = oauth2.initialize(builder);
        }
        return builder.build();
    }
}
