package org.pranay.aqueduct.sink.elasticsearch;

import org.pranay.aqueduct.exception.NeedToRetry;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.elasticsearch.request.EsRequestHandler;
import org.pranay.aqueduct.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Elastic search sink.
 */
public class EsSink extends AbstractSink {
    private RestHighLevelClient client;
    private EsRequestHandler esRequestHandler;
    private BulkRequest bulkRequest;
    private long esRequestTimeoutInMs;
    private Integer esWaitForActiveShardsCount;
    private List<String> esRetryStatusCodeBlacklist;

    /**
     * Instantiates a new Es sink.
     *
     * @param aqueductInstrumentation            the instrumentation
     * @param sinkType                   the sink type
     * @param client                     the client
     * @param esRequestHandler           the es request handler
     * @param esRequestTimeoutInMs       the es request timeout in ms
     * @param esWaitForActiveShardsCount the es wait for active shards count
     * @param esRetryStatusCodeBlacklist the es retry status code blacklist
     */
    public EsSink(AqueductInstrumentation aqueductInstrumentation, String sinkType, RestHighLevelClient client, EsRequestHandler esRequestHandler,
                  long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount, List<String> esRetryStatusCodeBlacklist) {
        super(aqueductInstrumentation, sinkType);
        this.client = client;
        this.esRequestHandler = esRequestHandler;
        this.esRequestTimeoutInMs = esRequestTimeoutInMs;
        this.esWaitForActiveShardsCount = esWaitForActiveShardsCount;
        this.esRetryStatusCodeBlacklist = esRetryStatusCodeBlacklist;
    }

    @Override
    protected void prepare(List<Message> messages) {
        bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMillis(esRequestTimeoutInMs));
        bulkRequest.waitForActiveShards(esWaitForActiveShardsCount);
        messages.forEach(message -> bulkRequest.add(esRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        BulkResponse bulkResponse = getBulkResponse();
        if (bulkResponse.hasFailures()) {
            getAqueductInstrumentation().logWarn("Bulk request failed");
            handleResponse(bulkResponse);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getAqueductInstrumentation().logInfo("Elastic Search connection closing");
        this.client.close();
    }

    BulkResponse getBulkResponse() throws IOException {
        return client.bulk(bulkRequest);
    }

    private void handleResponse(BulkResponse bulkResponse) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponseCount++;
                String responseStatus = String.valueOf(response.status().getStatus());
                if (esRetryStatusCodeBlacklist.contains(responseStatus)) {
                    getAqueductInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                    getAqueductInstrumentation().incrementCounter(Metrics.SINK_MESSAGES_DROP_TOTAL, "cause=" + response.status().name());
                    getAqueductInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
                } else {
                    throw new NeedToRetry(bulkResponse.buildFailureMessage());
                }
            }
        }
        getAqueductInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
