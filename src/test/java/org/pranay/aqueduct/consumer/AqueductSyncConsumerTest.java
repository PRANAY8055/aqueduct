package org.pranay.aqueduct.consumer;

import org.pranay.aqueduct.config.KafkaConsumerConfig;
import org.pranay.aqueduct.consumer.kafka.ConsumerAndOffsetManager;
import org.pranay.aqueduct.consumer.kafka.AqueductKafkaConsumer;
import org.pranay.aqueduct.consumer.kafka.OffsetManager;
import org.pranay.aqueduct.filter.FilteredMessages;
import org.pranay.aqueduct.filter.NoOpFilter;
import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.Sink;
import org.pranay.aqueduct.tracer.SinkTracer;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AqueductSyncConsumerTest {
    @Mock
    private AqueductKafkaConsumer aqueductKafkaConsumer;
    @Mock
    private Sink sink;
    @Mock
    private AqueductInstrumentation aqueductInstrumentation;
    @Mock
    private SinkTracer tracer;
    private AqueductSyncConsumer aqueductSyncConsumer;
    private List<Message> messages;

    @Before
    public void setUp() throws Exception {
        Message msg1 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        Message msg2 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2);
        OffsetManager offsetManger = new OffsetManager();
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), offsetManger, aqueductKafkaConsumer, kafkaConsumerConfig, aqueductInstrumentation);
        AqueductFilter aqueductFilter = new AqueductFilter(new NoOpFilter(aqueductInstrumentation), aqueductInstrumentation);
        aqueductSyncConsumer = new AqueductSyncConsumer(sink, tracer, consumerAndOffsetManager, aqueductFilter, aqueductInstrumentation);
        when(aqueductKafkaConsumer.readMessages()).thenReturn(messages);
    }

    @Test
    public void shouldProcessPartitions() throws IOException {
        aqueductSyncConsumer.process();
        verify(sink).pushMessage(messages);
    }

    @Test
    public void shouldProcessEmptyPartitions() throws IOException {
        when(aqueductKafkaConsumer.readMessages()).thenReturn(new ArrayList<>());
        aqueductSyncConsumer.process();
        verify(sink, times(0)).pushMessage(new ArrayList<>());
    }

    @Test
    public void shouldSendNoOfMessagesReceivedCount() throws IOException {
        aqueductSyncConsumer.process();
        verify(aqueductInstrumentation).logInfo("Processed {} records in consumer", 2);
    }

    @Test
    public void shouldCallTracerWithTheSpan() throws IOException {
        aqueductSyncConsumer.process();
        verify(sink).pushMessage(messages);
        verify(tracer).startTrace(messages);
        verify(tracer).finishTrace(any());
    }

    @Test
    public void shouldCloseConsumerIfConsumerIsNotNull() throws IOException {
        aqueductSyncConsumer.close();
        verify(aqueductInstrumentation, times(1)).logInfo("closing consumer");
        verify(tracer, times(1)).close();
        verify(aqueductKafkaConsumer, times(1)).close();
        verify(sink, times(1)).close();
        verify(aqueductInstrumentation, times(1)).close();
    }

    @Test
    public void shouldAddOffsetsForInvalidMessages() throws Exception {
        AqueductFilter aqueductFilter = Mockito.mock(AqueductFilter.class);
        ConsumerAndOffsetManager consumerAndOffsetManager = Mockito.mock(ConsumerAndOffsetManager.class);
        aqueductSyncConsumer = new AqueductSyncConsumer(sink, tracer, consumerAndOffsetManager, aqueductFilter, aqueductInstrumentation);
        Message msg1 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        Message msg2 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        Message msg3 = new Message(new byte[]{}, new byte[]{}, "topic", 0, 100);
        messages = Arrays.asList(msg1, msg2, msg3);
        Mockito.when(consumerAndOffsetManager.readMessages()).thenReturn(messages);
        Mockito.when(aqueductFilter.applyFilter(messages)).thenReturn(new FilteredMessages() {{
            addToValidMessages(msg3);
            addToValidMessages(msg1);
            addToInvalidMessages(msg2);
        }});
        Mockito.when(tracer.startTrace(messages)).thenReturn(new ArrayList<>());
        aqueductSyncConsumer.process();

        Mockito.verify(consumerAndOffsetManager, Mockito.times(1)).forceAddOffsetsAndSetCommittable(new ArrayList<Message>() {{
            add(msg2);
        }});
        Mockito.verify(sink, times(1)).pushMessage(new ArrayList<Message>() {{
            add(msg3);
            add(msg1);
        }});
        Mockito.verify(consumerAndOffsetManager, times(1)).addOffsetsAndSetCommittable(new ArrayList<Message>() {{
            add(msg3);
            add(msg1);
        }});
        Mockito.verify(consumerAndOffsetManager, times(1)).commit();

        verify(aqueductInstrumentation, times(1)).logInfo("Processed {} records in consumer", 3);
        verify(tracer, times(1)).startTrace(messages);
        verify(tracer, times(1)).finishTrace(new ArrayList<>());
        verify(aqueductInstrumentation, times(1)).captureDurationSince(eq(Metrics.SOURCE_KAFKA_PARTITIONS_PROCESS_TIME_MILLISECONDS), any(Instant.class));
    }

    @Test
    public void shouldNotCloseConsumerIfConsumerIsNull() throws IOException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), new OffsetManager(), null, kafkaConsumerConfig, aqueductInstrumentation);
        AqueductFilter aqueductFilter = new AqueductFilter(new NoOpFilter(aqueductInstrumentation), aqueductInstrumentation);
        aqueductSyncConsumer = new AqueductSyncConsumer(sink, tracer, consumerAndOffsetManager, aqueductFilter, aqueductInstrumentation);
        aqueductSyncConsumer.close();
        verify(aqueductInstrumentation, times(0)).logInfo("closing consumer");
        verify(tracer, times(1)).close();
        verify(aqueductKafkaConsumer, times(0)).close();
        verify(sink, times(1)).close();
        verify(aqueductInstrumentation, times(1)).close();
    }
}
