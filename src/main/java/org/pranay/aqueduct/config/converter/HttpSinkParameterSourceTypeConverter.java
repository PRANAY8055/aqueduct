package org.pranay.aqueduct.config.converter;

import org.pranay.aqueduct.config.enums.HttpSinkParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkParameterSourceTypeConverter implements Converter<HttpSinkParameterSourceType> {
    @Override
    public HttpSinkParameterSourceType convert(Method method, String input) {
        return HttpSinkParameterSourceType.valueOf(input.toUpperCase());
    }
}
