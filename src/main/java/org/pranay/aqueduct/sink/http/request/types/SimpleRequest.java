package org.pranay.aqueduct.sink.http.request.types;

import org.pranay.aqueduct.config.HttpSinkConfig;
import org.pranay.aqueduct.config.enums.HttpSinkParameterSourceType;
import org.pranay.aqueduct.config.enums.HttpSinkRequestMethodType;
import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.http.request.header.HeaderBuilder;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.http.request.body.JsonBody;
import org.pranay.aqueduct.sink.http.request.create.BatchRequestCreator;
import org.pranay.aqueduct.sink.http.request.create.IndividualRequestCreator;
import org.pranay.aqueduct.sink.http.request.create.RequestCreator;
import org.pranay.aqueduct.sink.http.request.entity.RequestEntityBuilder;
import org.pranay.aqueduct.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Simple request.
 */
public class SimpleRequest implements Request {

    private HttpSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpSinkRequestMethodType method;
    private RequestEntityBuilder requestEntityBuilder;
    private RequestCreator requestCreator;
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Simple request.
     *
     * @param statsDReporter the stats d reporter
     * @param config         the config
     * @param body           the body
     * @param method         the method
     */
    public SimpleRequest(StatsDReporter statsDReporter, HttpSinkConfig config, JsonBody body, HttpSinkRequestMethodType method) {
        this.httpSinkConfig = config;
        this.body = body;
        this.method = method;
        this.statsDReporter = statsDReporter;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(messages, requestEntityBuilder);
    }

    /**
     * Sets request strategy.
     *
     * @param headerBuilder        the header builder
     * @param uriBuilder           the uri builder
     * @param requestEntitybuilder the request entitybuilder
     * @return the request strategy
     */
    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        if (isTemplateBody(httpSinkConfig)) {
            this.requestCreator = new IndividualRequestCreator(new AqueductInstrumentation(
                    statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        } else {
            this.requestCreator = new BatchRequestCreator(new AqueductInstrumentation(
                    statsDReporter, BatchRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        }
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getSinkHttpServiceUrl().contains(",");
        return httpSinkConfig.getSinkHttpParameterSource() == HttpSinkParameterSourceType.DISABLED && !isDynamicUrl;
    }
}
