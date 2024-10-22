package com.ververica.flink.training.common;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * A ListInfoFactory that knows how to return TypeInformation for
 * a List<E>. This can be used with fields in a class that would
 * otherwise need to be serialized using Kryo.
 *
 * @param <E>
 */
@DoNotChangeThis
public class ListInfoFactory<E> extends TypeInfoFactory<List<E>> {

    @Override
    public TypeInformation<List<E>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        TypeInformation<?> elementType = genericParameters.get("E");
        if (elementType == null) {
            throw new InvalidTypesException("Type extraction is not possible on List (element type unknown).");
        }

        return Types.LIST((TypeInformation<E>) elementType);
    }
}
