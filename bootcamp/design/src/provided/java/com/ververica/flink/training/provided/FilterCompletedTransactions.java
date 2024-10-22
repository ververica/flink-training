package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@DoNotChangeThis
public class FilterCompletedTransactions extends ProcessWindowFunction<ShoppingCartRecord, AbandonedCartItem, String, TimeWindow> {

    @Override
    public void process(String transactionId, Context ctx, Iterable<ShoppingCartRecord> in, Collector<AbandonedCartItem> out) throws Exception {
        boolean foundCompleted = false;
        for (ShoppingCartRecord cart : in) {
            if (cart.isTransactionCompleted()) {
                foundCompleted = true;
                break;
            }
        }

        if (foundCompleted) {
            return;
        }

        TimestampedCollector tsc = (TimestampedCollector) out;
        for (ShoppingCartRecord cart : in) {
            String customerId = cart.getCustomerId();
            long transactionTime = cart.getTransactionTime();
            for (CartItem item : cart.getItems()) {
                tsc.setAbsoluteTimestamp(transactionTime);
                tsc.collect(new AbandonedCartItem(transactionId, transactionTime, customerId, item.getProductId()));
            }
        }
    }
}
