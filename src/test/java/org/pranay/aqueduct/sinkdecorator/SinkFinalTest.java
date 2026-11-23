package org.pranay.aqueduct.sinkdecorator;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.Sink;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SinkFinalTest {

    @Test
    public void shouldIgnoreMessages() throws IOException {
        Sink sink = Mockito.mock(Sink.class);
        AqueductInstrumentation aqueductInstrumentation = Mockito.mock(AqueductInstrumentation.class);
        SinkFinal sinkFinal = new SinkFinal(sink, aqueductInstrumentation);
        List<Message> messages = new ArrayList<Message>() {{
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
            add(new Message("".getBytes(), "".getBytes(), "", 0, 0));
        }};
        Mockito.when(sink.pushMessage(messages)).thenReturn(messages);

        sinkFinal.pushMessage(messages);
        Mockito.verify(aqueductInstrumentation, Mockito.times(1)).logInfo("Ignoring messages {}", 2);
        Mockito.verify(aqueductInstrumentation, Mockito.times(1)).captureGlobalMessageMetrics(Metrics.MessageScope.IGNORED, 2);
    }
}
