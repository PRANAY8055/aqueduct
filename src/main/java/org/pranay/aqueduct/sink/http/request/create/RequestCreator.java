package org.pranay.aqueduct.sink.http.request.create;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.sink.http.request.entity.RequestEntityBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Creates http requests.
 */
public interface RequestCreator {

    List<HttpEntityEnclosingRequestBase> create(List<Message> bodyContents, RequestEntityBuilder entity) throws URISyntaxException;
}
