package org.pranay.aqueduct.sink.http;


import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.common.AbstractHttpSink;
import org.pranay.aqueduct.sink.http.request.types.Request;
import org.pranay.stencil.client.StencilClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * HttpSink implement {@link AbstractHttpSink } lifecycle for HTTP.
 */
public class HttpSink extends AbstractHttpSink {

    private final Request request;

    /**
     * Instantiates a new Http sink.
     *
     * @param aqueductInstrumentation    the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public HttpSink(AqueductInstrumentation aqueductInstrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(aqueductInstrumentation, "http", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
        this.request = request;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException {
        try {
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        if (httpRequest.getMethod().equals("DELETE") && httpRequest.getEntity() == null) {
            return new ArrayList<>();
        }
        try (InputStream inputStream = httpRequest.getEntity().getContent()) {
            return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
        }
    }

    protected void captureMessageDropCount(HttpResponse response, List<String> contentStringList) {
        String requestBody = joptsimple.internal.Strings.join(contentStringList, "\n");

        List<String> result = Arrays.asList(requestBody.replaceAll("^\\[|]$", "").split("},\\s*\\{"));

        getAqueductInstrumentation().captureCount(Metrics.SINK_MESSAGES_DROP_TOTAL, (long) result.size(), "cause= " + statusCode(response));
        getAqueductInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }
}
