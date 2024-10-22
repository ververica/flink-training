package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.KeyedWindowResult;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Do aggregation of the preceding 1-minute window counts, to calculate per-5 minute item counts
 */
public class OneMinuteWindowCountAggregator implements AggregateFunction<KeyedWindowResult, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(KeyedWindowResult value, Long acc) {
        return acc + value.getResult();
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
