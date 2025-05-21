package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampDesignDetectionWorkflow;
import com.ververica.flink.training.provided.AbandonedCartItem;
import com.ververica.flink.training.provided.BootcampDesignMamboWorkflow;
import com.ververica.flink.training.provided.CountProductsAggregator;
import com.ververica.flink.training.provided.SetKeyAndTimeFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

public class BootcampDesignDetectionSolutionWorkflow extends BootcampDesignDetectionWorkflow {

    @Override
    public void build() {
        Preconditions.checkNotNull(abandonedStream, "abandonedStream must be set");
        Preconditions.checkNotNull(badCustomerSink, "badCustomerSink must be set");

        // Key by customer, tumbling window per minute.
        // Count number of unique products that were abandoned, then filter to
        // only the customers with too many abandoned products.
        final long abandonedLimit = maxAbandonedPerMinute;
        abandonedStream
                .keyBy(r -> r.getCustomerId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountProductsAggregator(), new SetKeyAndTimeFunction())
                .filter(r -> r.getResult() >= abandonedLimit)
                .sinkTo(badCustomerSink);
    }
}
