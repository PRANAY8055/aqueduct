package org.pranay.aqueduct.config.converter;

import org.pranay.aqueduct.sink.common.blobstorage.BlobStorageType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class BlobStorageTypeConverter implements Converter<BlobStorageType> {
    @Override
    public BlobStorageType convert(Method method, String input) {
        return BlobStorageType.valueOf(input.toUpperCase());
    }
}
