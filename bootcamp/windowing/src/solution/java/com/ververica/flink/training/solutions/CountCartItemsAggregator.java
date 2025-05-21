package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Simple aggregator that counts cart items
 */
public class CountCartItemsAggregator implements AggregateFunction<ShoppingCartRecord, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ShoppingCartRecord value, Long acc) {
        for (CartItem item : value.getItems()) {
            acc += item.getQuantity();
        }

        return acc;
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
