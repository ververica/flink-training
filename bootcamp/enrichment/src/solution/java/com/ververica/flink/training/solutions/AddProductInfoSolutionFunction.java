package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.ProductInfoRecord;
import com.ververica.flink.training.common.ProductRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class AddProductInfoSolutionFunction extends KeyedCoProcessFunction<String, ProductRecord, ProductInfoRecord, ProductRecord> {

    private transient ListState<ProductRecord> pendingLeft;
    private transient ValueState<ProductInfoRecord> pendingRight;

    @Override
    public void open(OpenContext ctx) throws Exception {
        pendingLeft = getRuntimeContext().getListState(new ListStateDescriptor<>("left", ProductRecord.class));
        pendingRight = getRuntimeContext().getState(new ValueStateDescriptor<>("right", ProductInfoRecord.class));
    }

    @Override
    public void processElement1(ProductRecord left, Context ctx, Collector<ProductRecord> out) throws Exception {
        ProductInfoRecord right = pendingRight.value();
        if (right != null) {
            left.setCategory(right.getCategory());
            left.setWeightKg(right.getWeightKg());
            left.setProductName(right.getProductName());
            out.collect(left);
        } else {
            // We need to wait for the product info record to arrive.
            // Note this wouldn't be safe to do if the state backend was HeapMapState, and
            // we had objectReuse enabled.
            pendingLeft.add(left);
        }
    }

    @Override
    public void processElement2(ProductInfoRecord right, Context ctx, Collector<ProductRecord> out) throws Exception {
        pendingRight.update(right);

        // If there are any pending left-side records, output the enriched version now.
        for (ProductRecord left : pendingLeft.get()) {
            left.setCategory(right.getCategory());
            left.setWeightKg(right.getWeightKg());
            left.setProductName(right.getProductName());
            out.collect(left);
        }

        pendingLeft.clear();
    }
}
