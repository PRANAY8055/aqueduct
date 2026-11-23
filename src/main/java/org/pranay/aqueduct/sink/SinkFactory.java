package org.pranay.aqueduct.sink;

import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.pranay.aqueduct.config.enums.SinkType;
import org.pranay.aqueduct.consumer.kafka.OffsetManager;
import org.pranay.aqueduct.exception.ConfigurationException;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.bigquery.BigquerySinkUtils;
import org.pranay.aqueduct.sink.blob.BlobSinkFactory;
import org.pranay.aqueduct.sink.elasticsearch.EsSinkFactory;
import org.pranay.aqueduct.sink.grpc.GrpcSinkFactory;
import org.pranay.aqueduct.sink.http.HttpSinkFactory;
import org.pranay.aqueduct.sink.influxdb.InfluxSinkFactory;
import org.pranay.aqueduct.sink.jdbc.JdbcSinkFactory;
import org.pranay.aqueduct.sink.mongodb.MongoSinkFactory;
import org.pranay.aqueduct.sink.prometheus.PromSinkFactory;
import org.pranay.depot.bigquery.BigQuerySink;
import org.pranay.depot.bigquery.BigQuerySinkFactory;
import org.pranay.depot.config.BigQuerySinkConfig;
import org.pranay.depot.config.RedisSinkConfig;
import org.pranay.depot.bigtable.BigTableSinkFactory;
import org.pranay.depot.bigtable.BigTableSink;
import org.pranay.depot.config.BigTableSinkConfig;
import org.pranay.depot.log.LogSink;
import org.pranay.depot.log.LogSinkFactory;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.depot.redis.RedisSink;
import org.pranay.depot.redis.RedisSinkFactory;
import org.pranay.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class SinkFactory {
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final StatsDReporter statsDReporter;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final StencilClient stencilClient;
    private final OffsetManager offsetManager;
    private final Map<String, String> config;
    private BigQuerySinkFactory bigQuerySinkFactory;
    private BigTableSinkFactory bigTableSinkFactory;
    private LogSinkFactory logSinkFactory;
    private RedisSinkFactory redisSinkFactory;

    public SinkFactory(KafkaConsumerConfig kafkaConsumerConfig,
                       StatsDReporter statsDReporter,
                       StencilClient stencilClient,
                       OffsetManager offsetManager) {
        aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, SinkFactory.class);
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.offsetManager = offsetManager;
        this.config = SinkFactoryUtils.addAdditionalConfigsForSinkConnectors(System.getenv());
    }

    /**
     * Initialization method for all the sinks.
     */
    public void init() {
        switch (this.kafkaConsumerConfig.getSinkType()) {
            case JDBC:
            case HTTP:
            case INFLUXDB:
            case ELASTICSEARCH:
            case GRPC:
            case PROMETHEUS:
            case BLOB:
            case MONGODB:
                return;
            case LOG:
                logSinkFactory = new LogSinkFactory(config, statsDReporter);
                logSinkFactory.init();
                return;
            case REDIS:
                redisSinkFactory = new RedisSinkFactory(
                        ConfigFactory.create(RedisSinkConfig.class, config),
                        statsDReporter);
                redisSinkFactory.init();
                return;
            case BIGQUERY:
                BigquerySinkUtils.addMetadataColumns(config);
                bigQuerySinkFactory = new BigQuerySinkFactory(
                        ConfigFactory.create(BigQuerySinkConfig.class, config),
                        statsDReporter,
                        BigquerySinkUtils.getRowIDCreator());
                bigQuerySinkFactory.init();
                return;
            case BIGTABLE:
                bigTableSinkFactory = new BigTableSinkFactory(
                        ConfigFactory.create(BigTableSinkConfig.class, config),
                        statsDReporter);
                bigTableSinkFactory.init();
                return;
            default:
                throw new ConfigurationException("Invalid Aqueduct SINK_TYPE");
        }
    }

    public Sink getSink() {
        SinkType sinkType = kafkaConsumerConfig.getSinkType();
        aqueductInstrumentation.logInfo("Sink Type: {}", sinkType);
        switch (sinkType) {
            case JDBC:
                return JdbcSinkFactory.create(config, statsDReporter, stencilClient);
            case HTTP:
                return HttpSinkFactory.create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return InfluxSinkFactory.create(config, statsDReporter, stencilClient);
            case LOG:
                return new GenericSink(new AqueductInstrumentation(statsDReporter, LogSink.class), sinkType.name(), logSinkFactory.create());
            case ELASTICSEARCH:
                return EsSinkFactory.create(config, statsDReporter, stencilClient);
            case REDIS:
                return new GenericSink(new AqueductInstrumentation(statsDReporter, RedisSink.class), sinkType.name(), redisSinkFactory.create());
            case GRPC:
                return GrpcSinkFactory.create(config, statsDReporter, stencilClient);
            case PROMETHEUS:
                return PromSinkFactory.create(config, statsDReporter, stencilClient);
            case BLOB:
                return BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
            case BIGQUERY:
                return new GenericSink(new AqueductInstrumentation(statsDReporter, BigQuerySink.class), sinkType.name(), bigQuerySinkFactory.create());
            case BIGTABLE:
                return new GenericSink(new AqueductInstrumentation(statsDReporter, BigTableSink.class), sinkType.name(), bigTableSinkFactory.create());
            case MONGODB:
                return MongoSinkFactory.create(config, statsDReporter, stencilClient);
            default:
                throw new ConfigurationException("Invalid Aqueduct SINK_TYPE");
        }
    }
}
