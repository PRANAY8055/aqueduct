package org.pranay.aqueduct.filter.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.pranay.aqueduct.config.FilterConfig;
import org.pranay.aqueduct.config.enums.FilterMessageFormatType;
import org.pranay.aqueduct.filter.Filter;
import org.pranay.aqueduct.filter.FilterException;
import org.pranay.aqueduct.filter.FilteredMessages;
import org.pranay.stencil.client.StencilClient;
import org.pranay.stencil.Parser;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static org.pranay.aqueduct.config.enums.FilterDataSourceType.KEY;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    private final FilterConfig filterConfig;
    private final AqueductInstrumentation aqueductInstrumentation;
    private final JsonSchema schema;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private JsonFormat.Printer jsonPrinter;
    private Parser parser;

    /**
     * Instantiates a new Json filter.
     *
     * @param filterConfig    the consumer config
     * @param aqueductInstrumentation the instrumentation
     */
    public JsonFilter(StencilClient stencilClient, FilterConfig filterConfig, AqueductInstrumentation aqueductInstrumentation) {
        this.aqueductInstrumentation = aqueductInstrumentation;
        this.filterConfig = filterConfig;
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        this.schema = schemaFactory.getSchema(filterConfig.getFilterJsonSchema());
        if (filterConfig.getFilterESBMessageFormat() == FilterMessageFormatType.PROTOBUF) {
            this.parser = stencilClient.getParser(filterConfig.getFilterSchemaProtoClass());
            this.jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
        }
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the json/protobuf records in binary format that are wrapped in {@link Message}
     * @return the list of filtered Messages
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        for (Message message : messages) {
            byte[] data = (filterConfig.getFilterDataSource().equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = deserialize(data);
            if (evaluate(jsonMessage)) {
                filteredMessages.addToValidMessages(message);
            } else {
                filteredMessages.addToInvalidMessages(message);
            }
        }
        return filteredMessages;
    }

    private boolean evaluate(String jsonMessage) throws FilterException {
        try {
            JsonNode message = objectMapper.readTree(jsonMessage);
            if (aqueductInstrumentation.isDebugEnabled()) {
                aqueductInstrumentation.logDebug("Json Message: \n {}", message.toPrettyString());
            }
            Set<ValidationMessage> validationErrors = schema.validate(message);
            validationErrors.forEach(error -> {
                aqueductInstrumentation.logDebug("Message filtered out due to: {}", error.getMessage());
            });
            return validationErrors.isEmpty();
        } catch (JsonProcessingException e) {
            throw new FilterException("Failed to parse JSON message", e);
        }
    }

    private String deserialize(byte[] data) throws FilterException {
        switch (filterConfig.getFilterESBMessageFormat()) {
            case PROTOBUF:
                try {
                    DynamicMessage message = parser.parse(data);
                    return jsonPrinter.print(message);

                } catch (Exception e) {
                    throw new FilterException("Failed to parse Protobuf message", e);
                }
            case JSON:
                return new String(data, Charset.defaultCharset());
            default:
                throw new FilterException("Invalid message format type");
        }
    }
}
