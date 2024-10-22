package com.ververica.flink.training.provided;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

public class CountProductsAggregator implements AggregateFunction<AbandonedCartItem, Set<String>, Long> {
    @Override
    public Set<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<String> add(AbandonedCartItem value, Set<String> acc) {
        acc.add(value.getProductId());
        return acc;
    }

    @Override
    public Long getResult(Set<String> acc) {
        return (long) acc.size();
    }

    @Override
    public Set<String> merge(Set<String> a, Set<String> b) {
        a.addAll(b);
        return a;
    }
}
