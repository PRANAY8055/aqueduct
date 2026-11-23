package org.pranay.aqueduct.sink.elasticsearch.request;

import org.pranay.aqueduct.config.EsSinkConfig;
import org.pranay.aqueduct.config.enums.EsSinkMessageType;
import org.pranay.aqueduct.config.enums.EsSinkRequestType;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

@AllArgsConstructor
public class EsRequestHandlerFactory {

    private EsSinkConfig esSinkConfig;
    private AqueductInstrumentation aqueductInstrumentation;
    private final String esIdFieldName;
    private final EsSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;
    private final String esRoutingKeyName;

    public EsRequestHandler getRequestHandler() {
        EsSinkRequestType esSinkRequestType = esSinkConfig.isSinkEsModeUpdateOnlyEnable() ? EsSinkRequestType.UPDATE_ONLY : EsSinkRequestType.INSERT_OR_UPDATE;
        aqueductInstrumentation.logInfo("ES request mode: {}", esSinkRequestType);

        ArrayList<EsRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new EsUpdateRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));
        esRequestHandlers.add(new EsUpsertRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));

        return esRequestHandlers
                .stream()
                .filter(EsRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esSinkRequestType.name() + " not supported"));
    }
}
