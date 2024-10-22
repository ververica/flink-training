package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.ProductRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

@DoNotChangeThis
public class SumWeightAggregator implements AggregateFunction<ProductRecord, Double, Double> {
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(ProductRecord value, Double acc) {
        return acc + (value.getWeightKg() * value.getQuantity());
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
