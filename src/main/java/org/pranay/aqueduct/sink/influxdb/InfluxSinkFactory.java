package org.pranay.aqueduct.sink.influxdb;


import org.pranay.aqueduct.config.InfluxSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.AbstractSink;
import org.pranay.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

/**
 * Influx sink factory create influx sink.
 */
public class InfluxSinkFactory {
    /**
     * Create Influx sink.
     *
     * @param configProperties the config properties
     * @param statsDReporter   the statsd reporter
     * @param stencilClient    the stencil client
     * @return Influx sink
     */
    public static AbstractSink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);

        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, InfluxSinkFactory.class);
        aqueductInstrumentation.logDebug("\nInflux Url: {}\nInflux Username: {}", config.getSinkInfluxUrl(), config.getSinkInfluxUsername());

        InfluxDB client = InfluxDBFactory.connect(config.getSinkInfluxUrl(), config.getSinkInfluxUsername(), config.getSinkInfluxPassword());
        aqueductInstrumentation.logInfo("InfluxDB connection established");

        return new InfluxSink(new AqueductInstrumentation(statsDReporter, InfluxSink.class), "influx.db", config, stencilClient.getParser(config.getInputSchemaProtoClass()), client, stencilClient);
    }
}
