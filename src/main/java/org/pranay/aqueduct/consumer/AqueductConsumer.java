package org.pranay.aqueduct.consumer;

import java.io.Closeable;
import java.io.IOException;

public interface AqueductConsumer extends Closeable {

    void process() throws IOException;
}
