package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ProductRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ExplodeShoppingCartFunction implements FlatMapFunction<ShoppingCartRecord, ProductRecord> {

    @Override
    public void flatMap(ShoppingCartRecord in, Collector<ProductRecord> out) throws Exception {
        // TODO - Need to call out.collect() for each item in our cart.
    }
}
