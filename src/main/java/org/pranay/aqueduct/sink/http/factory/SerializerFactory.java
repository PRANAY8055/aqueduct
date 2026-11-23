package org.pranay.aqueduct.sink.http.factory;

import org.pranay.aqueduct.config.HttpSinkConfig;
import org.pranay.aqueduct.config.enums.HttpSinkDataFormatType;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.serializer.JsonWrappedProtoByte;
import org.pranay.aqueduct.serializer.MessageSerializer;
import org.pranay.aqueduct.serializer.MessageToJson;
import org.pranay.aqueduct.serializer.MessageToTemplatizedJson;
import org.pranay.depot.metrics.StatsDReporter;
import org.pranay.stencil.client.StencilClient;
import org.pranay.stencil.Parser;
import lombok.AllArgsConstructor;

/**
 * SerializerFactory create json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

    private HttpSinkConfig httpSinkConfig;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;

    public MessageSerializer build() {
        AqueductInstrumentation aqueductInstrumentation = new AqueductInstrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            aqueductInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            Parser protoParser = stencilClient.getParser(httpSinkConfig.getInputSchemaProtoClass());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                aqueductInstrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return new MessageToJson(protoParser, false, httpSinkConfig.getSinkHttpSimpleDateFormatEnable());
            } else {
                aqueductInstrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return MessageToTemplatizedJson.create(new AqueductInstrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser);
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        aqueductInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getInputSchemaProtoClass() == null || httpSinkConfig.getInputSchemaProtoClass().equals("");
    }
}
