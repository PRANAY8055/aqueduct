package org.pranay.aqueduct.consumer;

import org.pranay.aqueduct.consumer.kafka.ConsumerAndOffsetManager;
import org.pranay.aqueduct.consumer.kafka.AqueductKafkaConsumer;
import org.pranay.aqueduct.consumer.kafka.OffsetManager;
import io.jaegertracing.Configuration;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.SinkFactory;
import org.pranay.aqueduct.utils.KafkaUtils;
import org.pranay.aqueduct.config.AppConfig;
import org.pranay.aqueduct.config.DlqConfig;
import org.pranay.aqueduct.config.FilterConfig;
import org.pranay.aqueduct.config.ErrorConfig;
import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.pranay.aqueduct.config.SinkPoolConfig;
import org.pranay.aqueduct.config.enums.KafkaConsumerMode;
import org.pranay.aqueduct.sink.SinkPool;
import org.pranay.aqueduct.filter.Filter;
import org.pranay.aqueduct.filter.NoOpFilter;
import org.pranay.aqueduct.filter.jexl.JexlFilter;
import org.pranay.aqueduct.filter.json.JsonFilter;
import org.pranay.aqueduct.filter.json.JsonFilterUtil;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.aqueduct.sink.common.KeyOrMessageParser;
import org.pranay.aqueduct.sinkdecorator.BackOff;
import org.pranay.aqueduct.sinkdecorator.BackOffProvider;
import org.pranay.aqueduct.error.ErrorHandler;
import org.pranay.aqueduct.sinkdecorator.ExponentialBackOffProvider;
import org.pranay.aqueduct.sinkdecorator.SinkFinal;
import org.pranay.aqueduct.sinkdecorator.SinkWithDlq;
import org.pranay.aqueduct.sinkdecorator.SinkWithFailHandler;
import org.pranay.aqueduct.sinkdecorator.SinkWithRetry;
import org.pranay.aqueduct.sink.dlq.DlqWriter;
import org.pranay.aqueduct.sink.dlq.DlqWriterFactory;
import org.pranay.aqueduct.tracer.SinkTracer;
import org.pranay.aqueduct.utils.StencilUtils;
import org.pranay.stencil.StencilClientFactory;
import org.pranay.stencil.client.StencilClient;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory for Aqueduct consumer.
 */
public class AqueductConsumerFactory {

    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final Map<String, String> config = System.getenv();
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final KeyOrMessageParser parser;
    private final OffsetManager offsetManager = new OffsetManager();

    /**
     * Instantiates a new Aqueduct consumer factory.
     *
     * @param kafkaConsumerConfig the kafka consumer config
     * @param statsDReporter      the stats d reporter
     */
    public AqueductConsumerFactory(KafkaConsumerConfig kafkaConsumerConfig, StatsDReporter statsDReporter) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        aqueductInstrumentation = new AqueductInstrumentation(this.statsDReporter, AqueductConsumerFactory.class);

        String additionalConsumerConfig = String.format(""
                        + "\n\tEnable Async Commit: %s"
                        + "\n\tCommit Only Current Partition: %s",
                this.kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable(),
                this.kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable());
        aqueductInstrumentation.logDebug(additionalConsumerConfig);

        String stencilUrl = this.kafkaConsumerConfig.getSchemaRegistryStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isSchemaRegistryStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, StencilUtils.getStencilConfig(kafkaConsumerConfig, statsDReporter.getClient()))
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(stencilClient.getParser(kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    private AqueductFilter buildFilter(FilterConfig filterConfig) {
        aqueductInstrumentation.logInfo("Filter Engine: {}", filterConfig.getFilterEngine());
        Filter filter;
        switch (filterConfig.getFilterEngine()) {
            case JSON:
                AqueductInstrumentation jsonFilterUtilAqueductInstrumentation = new AqueductInstrumentation(statsDReporter, JsonFilterUtil.class);
                JsonFilterUtil.logConfigs(filterConfig, jsonFilterUtilAqueductInstrumentation);
                JsonFilterUtil.validateConfigs(filterConfig, jsonFilterUtilAqueductInstrumentation);
                filter = new JsonFilter(stencilClient, filterConfig, new AqueductInstrumentation(statsDReporter, JsonFilter.class));
                break;
            case JEXL:
                filter = new JexlFilter(filterConfig, new AqueductInstrumentation(statsDReporter, JexlFilter.class));
                break;
            case NO_OP:
                filter = new NoOpFilter(new AqueductInstrumentation(statsDReporter, NoOpFilter.class));
                break;
            default:
                throw new IllegalArgumentException("Invalid filter engine type");
        }
        return new AqueductFilter(filter, new AqueductInstrumentation(statsDReporter, AqueductFilter.class));
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return AqueductConsumer aqueduct consumer
     */
    public AqueductConsumer buildConsumer() {
        FilterConfig filterConfig = ConfigFactory.create(FilterConfig.class, config);
        AqueductFilter aqueductFilter = buildFilter(filterConfig);
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceJaegarEnable()) {
            tracer = Configuration.fromEnv("Aqueduct" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        AqueductKafkaConsumer aqueductKafkaConsumer = KafkaUtils.createConsumer(kafkaConsumerConfig, config, statsDReporter, tracer);
        SinkTracer aqueductTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceJaegarEnable());
        SinkFactory sinkFactory = new SinkFactory(kafkaConsumerConfig, statsDReporter, stencilClient, offsetManager);
        sinkFactory.init();
        if (kafkaConsumerConfig.getSourceKafkaConsumerMode().equals(KafkaConsumerMode.SYNC)) {
            Sink sink = createSink(tracer, sinkFactory);
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), offsetManager, aqueductKafkaConsumer, kafkaConsumerConfig, new AqueductInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            return new AqueductSyncConsumer(
                    sink,
                    aqueductTracer,
                    consumerAndOffsetManager,
                    aqueductFilter,
                    new AqueductInstrumentation(statsDReporter, AqueductSyncConsumer.class));
        } else {
            SinkPoolConfig sinkPoolConfig = ConfigFactory.create(SinkPoolConfig.class, config);
            int nThreads = sinkPoolConfig.getSinkPoolNumThreads();
            List<Sink> sinks = new ArrayList<>(nThreads);
            for (int ii = 0; ii < nThreads; ii++) {
                sinks.add(createSink(tracer, sinkFactory));
            }
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, aqueductKafkaConsumer, kafkaConsumerConfig, new AqueductInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            SinkPool sinkPool = new SinkPool(
                    new LinkedBlockingQueue<>(sinks),
                    sinks,
                    Executors.newCachedThreadPool(),
                    sinkPoolConfig.getSinkPoolQueuePollTimeoutMS());
            return new AqueductAsyncConsumer(
                    sinkPool,
                    aqueductTracer,
                    consumerAndOffsetManager,
                    aqueductFilter,
                    new AqueductInstrumentation(statsDReporter, AqueductAsyncConsumer.class));
        }
    }

    private Sink createSink(Tracer tracer, SinkFactory sinkFactory) {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, config));
        Sink baseSink = sinkFactory.getSink();
        Sink sinkWithFailHandler = new SinkWithFailHandler(baseSink, errorHandler);
        Sink sinkWithRetry = withRetry(sinkWithFailHandler, errorHandler);
        Sink sinkWithDLQ = withDlq(sinkWithRetry, tracer, errorHandler);
        return new SinkFinal(sinkWithDLQ, new AqueductInstrumentation(statsDReporter, SinkFinal.class));
    }

    public Sink withDlq(Sink sink, Tracer tracer, ErrorHandler errorHandler) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, config);
        if (!dlqConfig.getDlqSinkEnable()) {
            return sink;
        }
        DlqWriter dlqWriter = DlqWriterFactory.create(new HashMap<>(config), statsDReporter, tracer);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithDlq(
                sink,
                dlqWriter,
                backOffProvider,
                dlqConfig,
                errorHandler,
                new AqueductInstrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param sink         Sink To wrap with retry decorator
     * @param errorHandler error handler
     * @return Sink with retry decorator
     */
    private Sink withRetry(Sink sink, ErrorHandler errorHandler) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithRetry(sink, backOffProvider, new AqueductInstrumentation(statsDReporter, SinkWithRetry.class), appConfig, parser, errorHandler);
    }

    private BackOffProvider getBackOffProvider() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        return new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new AqueductInstrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new AqueductInstrumentation(statsDReporter, BackOff.class)));
    }
}
