package com.ververica.flink.training.common;

import java.io.Serializable;
import java.util.function.Function;

@DoNotChangeThis
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
