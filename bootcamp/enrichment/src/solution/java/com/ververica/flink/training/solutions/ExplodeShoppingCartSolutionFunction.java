package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ProductRecord;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ExplodeShoppingCartSolutionFunction implements FlatMapFunction<ShoppingCartRecord, ProductRecord> {

    @Override
    public void flatMap(ShoppingCartRecord in, Collector<ProductRecord> out) throws Exception {
        for (CartItem item : in.getItems()) {
            out.collect(new ProductRecord(in, item));
        }
    }
}
