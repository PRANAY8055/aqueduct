package org.pranay.aqueduct.sink.blob.writer.remote;

import org.pranay.aqueduct.config.BlobSinkConfig;
import org.pranay.aqueduct.metrics.BlobStorageMetrics;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.blob.Constants;
import org.pranay.aqueduct.sink.blob.writer.local.LocalFileMetadata;
import org.pranay.aqueduct.sink.common.blobstorage.BlobStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BlobStorageCheckerTest {

    private final BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final ExecutorService remoteUploadScheduler = Mockito.mock(ExecutorService.class);
    private final Set<BlobStorageWriterFutureHandler> remoteUploadFutures = new HashSet<>();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private BlobStorage blobStorage;
    private BlobStorageChecker worker;

    @Mock
    private BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
    @Mock
    private AqueductInstrumentation aqueductInstrumentation;
    private LocalFileMetadata localFileMetadata;
    private String objectName;

    @Before
    public void setup() {
        when(sinkConfig.getFilePartitionProtoTimestampTimezone()).thenReturn("UTC");
        when(sinkConfig.getFilePartitionTimeGranularityType()).thenReturn(Constants.FilePartitionType.HOUR);
        localFileMetadata = new LocalFileMetadata("/tmp", "/tmp/dt=2021-01-01/hr=10/random-filename", 10L, 10L, 128L);
        objectName = "dt=2021-01-01/hr=10/random-filename";
        worker = new BlobStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                remoteUploadScheduler,
                blobStorage,
                aqueductInstrumentation);
        toBeFlushedToRemotePaths.clear();
        flushedToRemotePaths.clear();
        remoteUploadFutures.clear();
    }

    @Test
    public void shouldNotAddToFlushedIfUploadIsStillRunning() {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());
        ArrayList<BlobStorageWriterFutureHandler> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(localFileMetadata, workerFutures.get(0).getLocalFileMetadata());
    }

    @Test
    public void shouldAddToFlushedIfUploadIsFinished() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());

        ArrayList<BlobStorageWriterFutureHandler> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(localFileMetadata, workerFutures.get(0).getLocalFileMetadata());

        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(19L);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, remoteUploadFutures.size());
        Assert.assertEquals(1, flushedToRemotePaths.size());
        Assert.assertNotNull(flushedToRemotePaths.peek());
        Assert.assertEquals(localFileMetadata.getFullPath(), flushedToRemotePaths.peek());
    }

    @Test
    public void shouldRecordMetricOfFileUploadedCount() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(10L);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();

        verify(aqueductInstrumentation, times(1)).incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
    }

    @Test
    public void shouldRecordMetricOfFileUploadBytes() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(10L);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
        verify(aqueductInstrumentation).captureCount(BlobStorageMetrics.FILE_UPLOAD_BYTES, localFileMetadata.getSize());
    }

    @Test
    public void shouldRecordMetricOfUploadDuration() throws ExecutionException, InterruptedException {
        long totalTime = 10;
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        when(f.get()).thenReturn(totalTime);
        worker.run();

        verify(aqueductInstrumentation, (times(1))).captureDuration(BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    @Test
    public void shouldRecordMetricOfUploadFailedCountWhenUploadFutureThrowsInterruptedException() {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        try {
            when(f.get()).thenThrow(new InterruptedException());
        } catch (InterruptedException | ExecutionException ignored) {
        }
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        try {
            worker.run();
        } catch (RuntimeException ignored) {
        }

        verify(aqueductInstrumentation, times(1)).incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL,
                Metrics.FAILURE_TAG,
                Metrics.tag(BlobStorageMetrics.BLOB_STORAGE_ERROR_TYPE_TAG, ""));
    }

    @Test(expected = BlobStorageFailedException.class)
    public void shouldThrowFailedException() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add(localFileMetadata);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenThrow(new ExecutionException(new IOException()));
        when(remoteUploadScheduler.submit(any(Callable.class))).thenReturn(f);
        worker.run();
    }
}
