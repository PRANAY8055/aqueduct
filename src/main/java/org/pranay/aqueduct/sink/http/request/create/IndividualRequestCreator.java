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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndividualRequestCreator implements RequestCreator {

    private HeaderBuilder headerBuilder;
    private JsonBody jsonBody;
    private HttpSinkRequestMethodType method;
    private UriBuilder uriBuilder;
    private AqueductInstrumentation aqueductInstrumentation;
    private HttpSinkConfig httpSinkConfig;

    public IndividualRequestCreator(AqueductInstrumentation aqueductInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody body, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.jsonBody = body;
        this.method = method;
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.httpSinkConfig = httpSinkConfig;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder entity) throws URISyntaxException {
        List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
        List<String> bodyContents = jsonBody.serialize(messages);
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            URI requestUrl = uriBuilder.build(message);
            HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(requestUrl, method);

            Map<String, String> headerMap = headerBuilder.build(message);
            headerMap.forEach(request::addHeader);
            if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
                request.setEntity(entity.buildHttpEntity(bodyContents.get(i)));

                aqueductInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                        requestUrl, headerMap, bodyContents.get(i), method);
            } else {
                aqueductInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                        requestUrl, headerMap, method);
            }
            requests.add(request);
        }
        return requests;
    }
}
