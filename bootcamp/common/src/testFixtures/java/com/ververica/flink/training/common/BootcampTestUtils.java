package com.ververica.flink.training.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@DoNotChangeThis
public class BootcampTestUtils {

    public static final long START_TIME = 0;

    public static ShoppingCartRecord createShoppingCart(ShoppingCartGenerator generator, String country) {
        ShoppingCartRecord result = (ShoppingCartRecord)generator.createShoppingCart();
        result.setCountry(country);
        result.getItems().add(generator.createCartItem(country));
        return result;
    }

    public static List<ShoppingCartRecord> makeCartRecords() {
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);
        List<ShoppingCartRecord> records = new ArrayList<>();

        // Add a record that should get ignored (not completed)
        ShoppingCartRecord r1 = createShoppingCart(generator, "US");
        r1.setTransactionId("r1");
        records.add(r1);

        // Add an initial + updated entry. Both should also get ignored in analysis, since neither
        // is completed.
        ShoppingCartRecord r2 = createShoppingCart(generator, "US");
        r2.setTransactionId("r2");
        r2.setTransactionTime(START_TIME + 500);
        records.add(r2);

        ShoppingCartRecord r2updated = new ShoppingCartRecord(r2);
        r2updated.setTransactionTime(r2.getTransactionTime() + 1000);
        r2updated.getItems().add(generator.createCartItem("US"));
        records.add(r2updated);

        // Create a completed record, with two items. We'll first create a record
        // with one item, not completed, and then add a second item for a completed
        // version of the same record.
        ShoppingCartRecord r3 = createShoppingCart(generator, "US");
        r3.setTransactionId("r3");
        CartItem c31 = r3.getItems().get(0);
        c31.setQuantity(2);
        r3.setTransactionTime(START_TIME + 1000);
        records.add(r3);

        ShoppingCartRecord r3completed = new ShoppingCartRecord(r3);
        CartItem c32 = generator.createCartItem("US");
        c32.setQuantity(1);
        r3completed.getItems().add(c32);
        r3completed.setTransactionTime(r3.getTransactionTime() + 2000);
        r3completed.setTransactionCompleted(true);
        records.add(r3completed);

        // Create a completed record, in a future window - since our windows
        // are in 1 minute intervals (tumbling).
        ShoppingCartRecord r4 = createShoppingCart(generator, "US");
        r4.setTransactionId("r4");
        CartItem c41 = r4.getItems().get(0);
        c41.setQuantity(1);
        r4.setTransactionTime(START_TIME + Duration.ofMinutes(5).toMillis());
        records.add(r4);

        ShoppingCartRecord r4completed = new ShoppingCartRecord(r4);
        r4completed.setTransactionTime(r4.getTransactionTime() + 3000);
        r4completed.setTransactionCompleted(true);
        records.add(r4completed);

        // Create a completed record for a different country
        ShoppingCartRecord r5 = createShoppingCart(generator, "MX");
        r5.setTransactionId("r5");
        CartItem c51 = r5.getItems().get(0);
        c51.setQuantity(10);
        r5.setTransactionTime(START_TIME);
        records.add(r5);

        ShoppingCartRecord r5completed = new ShoppingCartRecord(r5);
        r5completed.setTransactionCompleted(true);
        r5completed.setTransactionTime(r5.getTransactionTime() + 4000);
        records.add(r5completed);

        return records;
    }

    public static void validateOneMinuteResults(Collection<KeyedWindowResult> results) {
        assertThat(results).containsExactlyInAnyOrder(
                new KeyedWindowResult("US",Duration.ofMinutes(0).toMillis(), 3),
                new KeyedWindowResult("US", Duration.ofMinutes(5).toMillis(), 1),
                new KeyedWindowResult("MX", Duration.ofMinutes(0).toMillis(), 10)
        );
    }

    public static void validateFiveMinuteResults(Collection<WindowAllResult> results) {
        assertThat(results).containsExactlyInAnyOrder(
                // Combination of US & MX records in first 5 minute window
                new WindowAllResult(Duration.ofMinutes(0).toMillis(), 13),
                // Only one US record in second 5 minute window.
                new WindowAllResult(Duration.ofMinutes(5).toMillis(), 1)
        );
    }
    public static void validateLongestTransactionResults(Collection<KeyedWindowResult> results) {
        assertThat(results).containsExactlyInAnyOrder(
                new KeyedWindowResult("r3", START_TIME, 2000),
                new KeyedWindowResult("r4", START_TIME + Duration.ofMinutes(5).toMillis(), 3000),
                new KeyedWindowResult("r5", START_TIME, 4000)
        );
    }




}
