package com.ververica.flink.training.solutions;

import static com.ververica.flink.training.common.BootcampTestUtils.*;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.estimateSpend;
import static org.junit.jupiter.api.Assertions.*;

class CalcTotalUSDollarPriceSolutionFunctionTest {

    @Test
    public void testDollarToDollarPrices() throws Exception {
        final long startTime = 0;

        CalcTotalUSDollarPriceSolutionFunction mapFunction = new CalcTotalUSDollarPriceSolutionFunction(startTime);
        OneInputStreamOperatorTestHarness<ShoppingCartRecord, Tuple2<String, Double>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamMap<>(mapFunction));

        // Open the test harness (this will call the open() method of your RichMapFunction)
        testHarness.open();

        ShoppingCartGenerator generator = new ShoppingCartGenerator(startTime);
        ShoppingCartRecord r1 = createShoppingCart(generator, "US");

        // Push an element into the test harness
        testHarness.processElement(new StreamRecord<>(r1, 0));

        // Retrieve and verify the output
        Tuple2<String, Double> output = testHarness.extractOutputValues().get(0);

        assertEquals(getResultTuple(r1), output);

        testHarness.close();
    }

    @Test
    public void testYenToDollarPrices() throws Exception {
        final long startTime = 0;

        CalcTotalUSDollarPriceSolutionFunction mapFunction = new CalcTotalUSDollarPriceSolutionFunction(startTime);
        OneInputStreamOperatorTestHarness<ShoppingCartRecord, Tuple2<String, Double>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamMap<>(mapFunction));

        // Open the test harness (this will call the open() method of your RichMapFunction)
        testHarness.open();

        ShoppingCartGenerator generator = new ShoppingCartGenerator(startTime);
        ShoppingCartRecord r1 = createShoppingCart(generator, "JP");
        CartItem c12 = generator.createCartItem("JP");
        c12.setQuantity(1);
        r1.getItems().add(c12);

        // Push an element into the test harness
        testHarness.processElement(new StreamRecord<>(r1, 0));

        // Retrieve and verify the output
        Tuple2<String, Double> output = testHarness.extractOutputValues().get(0);

        assertEquals(getResultTuple(r1), output);

        testHarness.close();
    }

    private Tuple2<String, Double> getResultTuple(ShoppingCartRecord cartRecord) {
        return Tuple2.of(cartRecord.getCountry(), estimateSpend(cartRecord,
                cartRecord.getTransactionTime() - START_TIME));
    }

}