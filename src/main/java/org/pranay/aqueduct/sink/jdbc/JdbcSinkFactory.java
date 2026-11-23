package org.pranay.aqueduct.sink.jdbc;


import org.pranay.aqueduct.config.JdbcSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.proto.ProtoToFieldMapper;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.aqueduct.sink.AbstractSink;
import org.pranay.stencil.client.StencilClient;
import org.pranay.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the JDBC Sink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDReporter statsDReporter, StencilClient client)}
 * to obtain the JDBCSink sink implementation.
 */
public class JdbcSinkFactory {

    /**
     * Create JDBC sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param client         the client
     * @return the abstract sink
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient client) {
        JdbcSinkConfig jdbcSinkConfig = ConfigFactory.create(JdbcSinkConfig.class, configuration);

        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, JdbcSinkFactory.class);
        String dbConfig = String.format(""
                        + "\n\tJDBC URL: %s\n\tJDBC Username: %s\n\tJDBC Tablename: %s\n\tUnique keys: %s",
                jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(), jdbcSinkConfig.getSinkJdbcTableName(), jdbcSinkConfig.getSinkJdbcUniqueKeys());
        aqueductInstrumentation.logDebug(dbConfig);
        JdbcConnectionPool connectionPool = new HikariJdbcConnectionPool(jdbcSinkConfig.getSinkJdbcUrl(), jdbcSinkConfig.getSinkJdbcUsername(),
                jdbcSinkConfig.getSinkJdbcPassword(), jdbcSinkConfig.getSinkJdbcConnectionPoolMaxSize(),
                jdbcSinkConfig.getSinkJdbcConnectionPoolTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolIdleTimeoutMs(), jdbcSinkConfig.getSinkJdbcConnectionPoolMinIdle());
        aqueductInstrumentation.logInfo("JDBC Connection established");
        QueryTemplate queryTemplate = createQueryTemplate(jdbcSinkConfig, client);

        return new JdbcSink(new AqueductInstrumentation(statsDReporter, JdbcSink.class), "db", connectionPool, queryTemplate, client);
    }

    private static QueryTemplate createQueryTemplate(JdbcSinkConfig jdbcSinkConfig, StencilClient stencilClient) {
        Parser protoParser = stencilClient.getParser(jdbcSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, jdbcSinkConfig.getInputSchemaProtoToColumnMapping());
        return new QueryTemplate(jdbcSinkConfig, protoToFieldMapper);
    }
}
