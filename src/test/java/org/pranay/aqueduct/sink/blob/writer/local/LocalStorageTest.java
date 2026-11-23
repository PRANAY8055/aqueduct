package org.pranay.aqueduct.sink.blob.writer.local;


import com.google.protobuf.Descriptors;
import org.pranay.aqueduct.config.BlobSinkConfig;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.sink.blob.Constants;
import org.pranay.aqueduct.sink.blob.writer.local.policy.WriterPolicy;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LocalStorageTest {

    @Test
    public void shouldDeleteFiles() throws Exception {
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        List<Descriptors.FieldDescriptor> metadataFieldDescriptor = new ArrayList<>();
        List<WriterPolicy> policies = new ArrayList<>();
        AqueductInstrumentation aqueductInstrumentation = Mockito.mock(AqueductInstrumentation.class);
        LocalStorage storage = new LocalStorage(sinkConfig, null, metadataFieldDescriptor, policies, aqueductInstrumentation);
        LocalStorage spy = Mockito.spy(storage);
        Mockito.doNothing().when(spy).deleteLocalFile(Paths.get("/tmp/a"), Paths.get("/tmp/.a.crc"));
        Mockito.when(sinkConfig.getLocalFileWriterType()).thenReturn(Constants.WriterType.PARQUET);
        spy.deleteLocalFile("/tmp/a");
        Mockito.verify(spy, Mockito.times(1)).deleteLocalFile(Paths.get("/tmp/a"), Paths.get("/tmp/.a.crc"));
        Mockito.verify(aqueductInstrumentation, Mockito.times(1)).logInfo("Deleting Local File {}", Paths.get("/tmp/a"));
        Mockito.verify(aqueductInstrumentation, Mockito.times(1)).logInfo("Deleting Local File {}", Paths.get("/tmp/.a.crc"));
    }
}
