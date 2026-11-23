package org.pranay.aqueduct.sink.prometheus.request;


import org.pranay.aqueduct.config.PromSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.prometheus.builder.TimeSeriesBuilder;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.prometheus.builder.HeaderBuilder;
import org.pranay.aqueduct.sink.prometheus.builder.RequestEntityBuilder;
import org.pranay.aqueduct.sink.prometheus.builder.WriteRequestBuilder;
import org.pranay.stencil.Parser;

/**
 * Prometheus Request Creator.
 */
public class PromRequestCreator {
    private PromSinkConfig promSinkConfig;
    private StatsDReporter statsDReporter;
    private Parser protoParser;


    /**
     * Instantiates a new Prometheus request creator.
     *
     * @param statsDReporter the statsd reporter
     * @param promSinkConfig the configuration for prometheus sink
     * @param protoParser    the proto parser
     */
    public PromRequestCreator(StatsDReporter statsDReporter, PromSinkConfig promSinkConfig, Parser protoParser) {
        this.statsDReporter = statsDReporter;
        this.promSinkConfig = promSinkConfig;
        this.protoParser = protoParser;
    }

    /**
     * Instantiates a new Prometheus request.
     *
     * @return PromRequest
     */
    public PromRequest createRequest() {
        WriteRequestBuilder body = createBody();
        HeaderBuilder headerBuilder = new HeaderBuilder(promSinkConfig.getSinkPromHeaders());
        String baseUrl = promSinkConfig.getSinkPromServiceUrl();
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        return new PromRequest(new AqueductInstrumentation(statsDReporter, PromRequest.class),
                headerBuilder, baseUrl, requestEntityBuilder, body);
    }

    /**
     * create prometheus writeRequest builder.
     *
     * @return WriteRequestBuilder
     */
    private WriteRequestBuilder createBody() {
        TimeSeriesBuilder timeSeriesBuilder = new TimeSeriesBuilder(promSinkConfig);
        return new WriteRequestBuilder(timeSeriesBuilder, protoParser);
    }
}
