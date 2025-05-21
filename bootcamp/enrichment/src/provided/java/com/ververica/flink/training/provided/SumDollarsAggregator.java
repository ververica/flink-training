package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@DoNotChangeThis
public class SumDollarsAggregator implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(Tuple2<String, Double> value, Double acc) {
        return acc + value.f1;
    }

    @Override
    public Double getResult(Double acc) {
        return acc;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
