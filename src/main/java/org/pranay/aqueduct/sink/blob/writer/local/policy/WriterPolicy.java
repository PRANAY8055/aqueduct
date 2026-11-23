package org.pranay.aqueduct.sink.blob.writer.local.policy;

import org.pranay.aqueduct.sink.blob.writer.local.LocalFileMetadata;

public interface WriterPolicy {
    boolean shouldRotate(LocalFileMetadata metadata);
}
