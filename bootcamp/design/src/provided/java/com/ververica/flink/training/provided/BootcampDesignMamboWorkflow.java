package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.KeyedWindowResult;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

@DoNotChangeThis
public class BootcampDesignMamboWorkflow {

    private static final long DEFAULT_ABANDONED_LIMIT = 1;

    private DataStream<ShoppingCartRecord> cartStream;
    private Sink<KeyedWindowResult> badCustomerSink;
    private Sink<KeyedWindowResult> analyticsSink;
    private long maxAbandonedPerMinute = DEFAULT_ABANDONED_LIMIT;

    public BootcampDesignMamboWorkflow() {
    }

    public BootcampDesignMamboWorkflow setCartStream(DataStream<ShoppingCartRecord> cartStream) {
        this.cartStream = cartStream;
        return this;
    }

    public BootcampDesignMamboWorkflow setAnalyticsSink(Sink<KeyedWindowResult> analyticsSink) {
        this.analyticsSink = analyticsSink;
        return this;
    }


    public BootcampDesignMamboWorkflow setBadCustomerSink(Sink<KeyedWindowResult> badCustomerSink) {
        this.badCustomerSink = badCustomerSink;
        return this;
    }

    public BootcampDesignMamboWorkflow setMaxAbandonedPerMinute(long maxAbandonedPerMinute) {
        this.maxAbandonedPerMinute = maxAbandonedPerMinute;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(cartStream, "cartStream must be set");
        Preconditions.checkNotNull(analyticsSink, "analyticsSink must be set");
        Preconditions.checkNotNull(badCustomerSink, "badCustomerSink must be set");

        // Assign timestamps & watermarks. Note that we don't filter out pending
        // transactions, as those are the ones we care about.
        DataStream<ShoppingCartRecord> watermarked = cartStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingCartRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTransactionTime()));

        // Key by transactionId, and filter out completed transactions. Output
        // AbandonedCartItem(transactionId, transactionTime, customerId, productId) records.
        DataStream<AbandonedCartItem> abandoned = watermarked
                .keyBy(r -> r.getTransactionId())
                .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(5)))
                .process(new FilterCompletedTransactions());

        // Key by customer id, tumbling window per hour. Return unique transactionIds per customer
        abandoned
                .keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new CountTransactionsAggregator(), new SetKeyAndTimeFunction())
                .sinkTo(analyticsSink);

        // Key by customer, tumbling window per minute.
        // Count number of unique products that were abandoned, then filter to
        // only the customers with too many abandoned products.
        final long abandonedLimit = maxAbandonedPerMinute;
        abandoned
                .keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountProductsAggregator(), new SetKeyAndTimeFunction())
                .filter(r -> r.getResult() >= abandonedLimit)
                .sinkTo(badCustomerSink);
    }

}

