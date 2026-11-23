package org.pranay.aqueduct.filter.json;

import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.config.FilterConfig;
import lombok.experimental.UtilityClass;

import static org.pranay.aqueduct.config.enums.FilterMessageFormatType.PROTOBUF;

/**
 * The type Json filter util.
 */
@UtilityClass
public class JsonFilterUtil {

    /**
     * Log configs.
     *
     * @param filterConfig    the filter config
     * @param aqueductInstrumentation the instrumentation
     */
    public static void logConfigs(FilterConfig filterConfig, AqueductInstrumentation aqueductInstrumentation) {
        aqueductInstrumentation.logInfo("\n\tFilter data source type: {}", filterConfig.getFilterDataSource());
        aqueductInstrumentation.logInfo("\n\tFilter JSON Schema: {}", filterConfig.getFilterJsonSchema());
        aqueductInstrumentation.logInfo("\n\tFilter ESB message format: {}", filterConfig.getFilterESBMessageFormat());
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF) {
            aqueductInstrumentation.logInfo("\n\tMessage Proto class: {}", filterConfig.getFilterSchemaProtoClass());
        }
    }

    /**
     * Validate configs.
     *
     * @param filterConfig    the filter config
     * @param aqueductInstrumentation the instrumentation
     */
    public static void validateConfigs(FilterConfig filterConfig, AqueductInstrumentation aqueductInstrumentation) {
        if (filterConfig.getFilterJsonSchema() == null) {
            aqueductInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter JSON Schema is invalid");
        }
        if (filterConfig.getFilterESBMessageFormat() == null) {
            aqueductInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter ESB message type cannot be null");
        }
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF && filterConfig.getFilterSchemaProtoClass() == null) {
            aqueductInstrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Proto Schema class cannot be null");
        }
    }
}
