package org.pranay.aqueduct.config.converter;

import org.pranay.aqueduct.sink.dlq.DLQWriterType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class DlqWriterTypeConverter implements Converter<DLQWriterType> {
    @Override
    public DLQWriterType convert(Method method, String input) {
        return DLQWriterType.valueOf(input.toUpperCase());
    }
}
