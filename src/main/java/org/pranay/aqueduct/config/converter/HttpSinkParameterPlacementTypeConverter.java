package org.pranay.aqueduct.config.converter;

import org.pranay.aqueduct.config.enums.HttpSinkParameterPlacementType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpSinkParameterPlacementTypeConverter implements Converter<HttpSinkParameterPlacementType> {
    @Override
    public HttpSinkParameterPlacementType convert(Method method, String input) {
        return HttpSinkParameterPlacementType.valueOf(input.toUpperCase());
    }
}
