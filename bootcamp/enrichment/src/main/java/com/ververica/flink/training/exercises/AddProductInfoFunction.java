package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.ProductInfoRecord;
import com.ververica.flink.training.common.ProductRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class AddProductInfoFunction extends KeyedCoProcessFunction<String, ProductRecord, ProductInfoRecord, ProductRecord> {

    // We need to keep track of all product records that arrive before the corresponding
    // ProductInfoRecord.
    private transient ListState<ProductRecord> pendingLeft;

    // We need to keep track of the ProductInfoRecord, so we can use it to enrich
    // subsequent ProductRecords.
    private transient ValueState<ProductInfoRecord> pendingRight;

    @Override
    public void open(OpenContext ctx) throws Exception {
        pendingLeft = getRuntimeContext().getListState(new ListStateDescriptor<>("left", ProductRecord.class));
        pendingRight = getRuntimeContext().getState(new ValueStateDescriptor<>("right", ProductInfoRecord.class));
    }

    @Override
    public void processElement1(ProductRecord left, Context ctx, Collector<ProductRecord> out) throws Exception {
        // TODO - if we have a ProductInfoRecord in state, use it to enrich the category, weight, and
        // product names and output the enriched result. Otherwise we have to add the ProductRecord to
        // our state, so we keep it around until we get the ProductInfoRecord.
        // TODO - make it so.
    }

    @Override
    public void processElement2(ProductInfoRecord right, Context ctx, Collector<ProductRecord> out) throws Exception {
        // Update our state with this record.
        // TODO - make it so.

        // If there are any pending left-side records, output the enriched version now, and then
        // clear out that state when we're done with it.
        // TODO - make it so.
    }
}
