package org.pranay.aqueduct.sink.elasticsearch.request;

import org.pranay.aqueduct.config.EsSinkConfig;
import org.pranay.aqueduct.config.enums.EsSinkMessageType;
import org.pranay.aqueduct.config.enums.EsSinkRequestType;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.serializer.MessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsRequestHandlerFactoryTest {

    @Mock
    private EsSinkConfig esSinkConfig;

    @Mock
    private AqueductInstrumentation aqueductInstrumentation;

    private MessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnInsertRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(false);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, aqueductInstrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(aqueductInstrumentation, times(1)).logInfo("ES request mode: {}", EsSinkRequestType.INSERT_OR_UPDATE);
        assertEquals(EsUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(true);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, aqueductInstrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(aqueductInstrumentation, times(1)).logInfo("ES request mode: {}", EsSinkRequestType.UPDATE_ONLY);
        assertEquals(EsUpdateRequestHandler.class, requestHandler.getClass());
    }
}
