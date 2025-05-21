package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampEnrichment1Workflow;
import com.ververica.flink.training.exercises.BootcampEnrichment2Workflow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BootcampEnrichmentWorkflowTestUtils {

    // The beginning time for our workflow, for events
    private static final long START_TIME = 0;

    public static void testAddingUSDEquivalentWorkflow(BootcampEnrichment1Workflow workflow) throws Exception {
        List<ShoppingCartRecord> records = BootcampTestUtils.makeCartRecords();

        ResultsSink sink = new ResultsSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow
                .setCartStream(env.fromData(records).setParallelism(1))
                .setResultSink(sink)
                // Set up start time used by currency exchange rate API
                .setStartTime(START_TIME)
                .build();

        env.execute("BootcampEnrichment1Workflow");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(records, "r3", Duration.ofMinutes(0))),
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(5).toMillis(),
                        estimateSpend(records, "r4", Duration.ofMinutes(5))),
                new KeyedWindowDouble("MX", START_TIME + Duration.ofMinutes(0).toMillis(),
                        estimateSpend(records, "r5", Duration.ofMinutes(0)))
        );
    }

    public static double estimateSpend(List<ShoppingCartRecord> records, String transactionId, Duration currentRateTime) {
        // Find the cart by transactionId.
        ShoppingCartRecord cartRecord = null;
        for (ShoppingCartRecord cart : records) {
            if (cart.isTransactionCompleted() && (cart.getTransactionId().equals(transactionId))) {
                cartRecord = cart;
                break;
            }
        }
        assertThat(cartRecord).isNotNull();

        return estimateSpend(cartRecord, currentRateTime);
    }

    public static double estimateSpend(ShoppingCartRecord cartRecord, long currentRateTime) {
        return estimateSpend(cartRecord, Duration.ofMillis(currentRateTime));
    }

    public static double estimateSpend(ShoppingCartRecord cartRecord, Duration currentRateTime) {
        String country = cartRecord.getCountry();
        CurrencyRateAPI api = new CurrencyRateAPI(START_TIME);
        double rate = api.getRate(country, currentRateTime);
        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (item.getPrice() * rate) * item.getQuantity();
        }

        return result;
    }

    private static class ResultsSink extends MockSink<KeyedWindowDouble> {

        private static final ConcurrentLinkedQueue<KeyedWindowDouble> QUEUE = new ConcurrentLinkedQueue<>();

        @Override
        public ConcurrentLinkedQueue<KeyedWindowDouble> getSink() {
            return QUEUE;
        }
    }

    public static void testAddingProductWeightWorkflow(BootcampEnrichment2Workflow workflow) throws Exception {
        List<ShoppingCartRecord> carts = BootcampTestUtils.makeCartRecords();

        // Generate product info for every product.
        List<ProductInfoRecord> products = new ArrayList<>();
        ProductInfoGenerator productGenerator = new ProductInfoGenerator();
        for (long i = 0; i < ProductInfoGenerator.NUM_UNIQUE_PRODUCTS; i++) {
            products.add(productGenerator.apply(i));
        }

        ResultsSink sink = new ResultsSink();

        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);
        workflow
                .setCartStream(env.fromData(carts).setParallelism(1))
                .setProductInfoStream(env.fromData(products).setParallelism(1))
                .setResultSink(sink)
                .build();

        env.execute("BootcampWindowingSolution2Job");

        // Validate we get the expected results.
        assertThat(sink.getSink()).containsExactlyInAnyOrder(
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(0).toMillis(),
                        calcTotalWeight(carts, "r3", productGenerator)),
                new KeyedWindowDouble("US",START_TIME + Duration.ofMinutes(5).toMillis(),
                        calcTotalWeight(carts, "r4", productGenerator)),
                new KeyedWindowDouble("MX", START_TIME + Duration.ofMinutes(0).toMillis(),
                        calcTotalWeight(carts, "r5", productGenerator))
        );

    }

    private static double calcTotalWeight(List<ShoppingCartRecord> records, String transactionId, ProductInfoGenerator productGenerator) {
        ShoppingCartRecord cartRecord = null;
        for (ShoppingCartRecord cart : records) {
            if (cart.isTransactionCompleted() && (cart.getTransactionId().equals(transactionId))) {
                cartRecord = cart;
                break;
            }
        }
        assertThat(cartRecord).isNotNull();

        double result = 0.0;
        for (CartItem item : cartRecord.getItems()) {
            result += (productGenerator.getProductWeight(item.getProductId()) * item.getQuantity());
        }

        return result;
    }


}