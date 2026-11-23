package org.pranay.aqueduct.sink.jdbc;


import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.AbstractSink;
import org.pranay.stencil.client.StencilClient;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC Sink allows messages consumed from kafka to be persisted to a database.
 * The related configurations for JDBC Sink can be found here: {@see org.pranay.aqueduct.config.JdbcSinkConfig}
 */
public class JdbcSink extends AbstractSink {

    private JdbcConnectionPool pool;
    private QueryTemplate queryTemplate;
    private StencilClient stencilClient;
    private Statement statement;
    private Connection connection = null;

    /**
     * Instantiates a new Jdbc sink.
     *
     * @param aqueductInstrumentation the instrumentation
     * @param sinkType        the sink type
     * @param pool            the pool
     * @param queryTemplate   the query template
     * @param stencilClient   the stencil client
     */
    public JdbcSink(AqueductInstrumentation aqueductInstrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient) {
        super(aqueductInstrumentation, sinkType);
        this.pool = pool;
        this.queryTemplate = queryTemplate;
        this.stencilClient = stencilClient;
    }

    JdbcSink(AqueductInstrumentation aqueductInstrumentation, String sinkType, JdbcConnectionPool pool, QueryTemplate queryTemplate, StencilClient stencilClient, Statement statement, Connection connection) {
        this(aqueductInstrumentation, sinkType, pool, queryTemplate, stencilClient);
        this.statement = statement;
        this.connection = connection;
    }

    @Override
    protected void prepare(List<Message> messages) throws SQLException {
        List<String> queriesList = createQueries(messages);
        connection = pool.getConnection();
        statement = connection.createStatement();

        for (String query : queriesList) {
            statement.addBatch(query);
        }
    }

    protected List<String> createQueries(List<Message> messages) {
        List<String> queries = new ArrayList<>();
        for (Message message : messages) {
            String queryString = queryTemplate.toQueryString(message);
            getAqueductInstrumentation().logDebug(queryString);
            queries.add(queryString);
        }
        return queries;
    }

    @Override
    protected List<Message> execute() throws Exception {
        try {
            int[] updateCounts = statement.executeBatch();
            getAqueductInstrumentation().logDebug("DB response: {}", Arrays.toString(updateCounts));
        } finally {
            if (connection != null) {
                pool.release(connection);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        try {
            getAqueductInstrumentation().logInfo("Database connection closing");
            pool.shutdown();
            stencilClient.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
