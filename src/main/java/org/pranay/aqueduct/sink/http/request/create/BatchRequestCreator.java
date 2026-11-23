package org.pranay.aqueduct.sink.http.request.create;

import org.pranay.aqueduct.config.HttpSinkConfig;
import org.pranay.aqueduct.config.enums.HttpSinkRequestMethodType;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.http.request.entity.RequestEntityBuilder;
import org.pranay.aqueduct.sink.http.request.header.HeaderBuilder;
import org.pranay.aqueduct.sink.http.request.HttpRequestMethodFactory;
import org.pranay.aqueduct.sink.http.request.body.JsonBody;
import org.pranay.aqueduct.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchRequestCreator implements RequestCreator {

    private UriBuilder uriBuilder;
    private HeaderBuilder headerBuilder;
    private HttpSinkRequestMethodType method;
    private JsonBody jsonBody;
    private AqueductInstrumentation aqueductInstrumentation;
    private HttpSinkConfig httpSinkConfig;

    public BatchRequestCreator(AqueductInstrumentation aqueductInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.httpSinkConfig = httpSinkConfig;
        this.aqueductInstrumentation = aqueductInstrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String messagesString = jsonBody.serialize(messages).toString();

        if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
            request.setEntity(requestEntityBuilder.buildHttpEntity(messagesString));
            aqueductInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    uri, headerMap, jsonBody.serialize(messages), method);
        } else {
            aqueductInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                    uri, headerMap, method);
        }
        return Collections.singletonList(request);
    }
}
