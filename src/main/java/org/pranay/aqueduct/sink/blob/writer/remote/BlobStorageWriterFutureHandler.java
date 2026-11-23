package org.pranay.aqueduct.sink.blob.writer.remote;

import org.pranay.aqueduct.metrics.BlobStorageMetrics;
import org.pranay.aqueduct.metrics.AqueductInstrumentation;
import org.pranay.aqueduct.metrics.Metrics;
import org.pranay.aqueduct.sink.blob.writer.local.LocalFileMetadata;
import org.pranay.aqueduct.sink.common.blobstorage.BlobStorageException;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@AllArgsConstructor
@Data
public class BlobStorageWriterFutureHandler {
    private Future<Long> future;
    private LocalFileMetadata localFileMetadata;
    private AqueductInstrumentation aqueductInstrumentation;
    private static final String EMPTY = "";

    public String getFullPath() {
        return localFileMetadata.getFullPath();
    }

    public boolean isFinished() {
        if (!future.isDone()) {
            return false;
        }
        try {
            long totalTime = future.get();
            captureFileUploadSuccessMetric(totalTime);
            return true;
        } catch (InterruptedException e) {
            captureUploadFailedMetric(e);
            throw new BlobStorageFailedException(e);
        } catch (ExecutionException e) {
            captureUploadFailedMetric(e.getCause());
            throw new BlobStorageFailedException(e.getCause());
        }
    }

    private void captureFileUploadSuccessMetric(long totalTime) {
        aqueductInstrumentation.logInfo("Flushed to blob storage {}", localFileMetadata.getFullPath());
        aqueductInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
        aqueductInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_BYTES, localFileMetadata.getSize());
        aqueductInstrumentation.captureCount(BlobStorageMetrics.FILE_UPLOAD_RECORDS_TOTAL, localFileMetadata.getRecordCount());
        aqueductInstrumentation.captureDuration(BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    private void captureUploadFailedMetric(Throwable e) {
        aqueductInstrumentation.logError("Failed to flush to blob storage {}", e.getMessage());
        String errorType;
        if (e instanceof BlobStorageException) {
            errorType = ((BlobStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        aqueductInstrumentation.incrementCounter(BlobStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.FAILURE_TAG, Metrics.tag(BlobStorageMetrics.BLOB_STORAGE_ERROR_TYPE_TAG, errorType));
    }
}
