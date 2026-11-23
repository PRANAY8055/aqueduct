package org.pranay.aqueduct.config.converter;

import org.pranay.aqueduct.config.enums.SinkType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SinkTypeConverter implements Converter<SinkType> {
    @Override
    public SinkType convert(Method method, String input) {
        return SinkType.valueOf(input.toUpperCase());
    }
}
