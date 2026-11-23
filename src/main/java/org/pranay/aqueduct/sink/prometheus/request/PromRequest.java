package org.pranay.aqueduct.sink.prometheus.request;

import org.pranay.aqueduct.exception.DeserializerException;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.prometheus.builder.HeaderBuilder;
import cortexpb.Cortex;
import org.pranay.aqueduct.sink.prometheus.builder.RequestEntityBuilder;
import org.pranay.aqueduct.sink.prometheus.builder.WriteRequestBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Prometheus request create one HttpPost per batch messages.
 */
public class PromRequest {
    private AqueductInstrumentation aqueductInstrumentation;
    private WriteRequestBuilder writeRequestBuilder;
    private String url;
    private RequestEntityBuilder requestEntityBuilder;
    private HeaderBuilder headerBuilder;


    /**
     * Instantiates a new Prometheus request.
     *
     * @param aqueductInstrumentation      the instrumentation
     * @param headerBuilder        the header builder
     * @param url                  the url
     * @param requestEntityBuilder the request entity builder
     * @param writeRequestBuilder  the writeRequest builder
     */
    public PromRequest(AqueductInstrumentation aqueductInstrumentation, HeaderBuilder headerBuilder, String url,
                       RequestEntityBuilder requestEntityBuilder, WriteRequestBuilder writeRequestBuilder) {
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.writeRequestBuilder = writeRequestBuilder;
        this.headerBuilder = headerBuilder;
        this.url = url;
        this.requestEntityBuilder = requestEntityBuilder;
    }

    /**
     * build Prometheus request.
     *
     * @param messages the list of consumer message
     * @return HttpEntityEnclosingRequestBase
     * @throws DeserializerException the exception on deserialization
     * @throws URISyntaxException    the exception on URI
     * @throws IOException           the io exception
     */
    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException, IOException {
        Cortex.WriteRequest writeRequest = writeRequestBuilder.buildWriteRequest(messages);
        URI uri = new URI(url);
        HttpEntityEnclosingRequestBase request = new HttpPost(uri);
        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        request.setEntity(requestEntityBuilder.buildHttpEntity(writeRequest));
        return Collections.singletonList(request);
    }
}
