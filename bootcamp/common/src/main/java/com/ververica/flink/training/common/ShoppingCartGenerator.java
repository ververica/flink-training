package com.ververica.flink.training.common;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A generator that can be used with a FakeParallelSource to generate ShoppingCartRecords.
 */
@DoNotChangeThis
public class ShoppingCartGenerator implements SerializableFunction<Long, ShoppingCartRecord> {

    private static final int MAX_ACTIVE_CARTS = 100;
    private static final long DEFAULT_PER_TRANSACTION_GAP_MS = 10;

    public static final String[] COUNTRIES = new String[] {
            "US",
            "MX",
            "CN",
            "CA",
            "JP",
    };

    private static final String[] PAYMENT_METHODS = new String[] {
            "CC",
            "PayPal",
            "Zelle",
            "Debit",
    };

    private static final double MIN_PRICE = 0.50;
    private static final double MAX_PRICE = 100.00;

    private static final String[] STREET_NAMES = {"Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill", "Park"};
    private static final String[] STREET_TYPES = {"St", "Ave", "Blvd", "Rd", "Ln", "Dr", "Way", "Pl", "Ct", "Terrace"};
    private static final String[] CITIES = {"Springfield", "Franklin", "Clinton", "Greenville", "Bristol", "Fairview", "Salem", "Madison", "Georgetown", "Arlington"};
    private static final String[] STATES = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"};

    public static final int MAX_PRODUCT_QUANTITY = 10;

    private long curTime;

    private long transactionGap;

    private Random rand = new Random();

    private final List<ShoppingCartRecord> activeCarts = new ArrayList<>();

    public ShoppingCartGenerator(long startingTime) {
        this(startingTime, DEFAULT_PER_TRANSACTION_GAP_MS, System.currentTimeMillis());
    }

    public ShoppingCartGenerator(long startingTime, long transactionGap, long seed) {
        this.curTime = startingTime;
        this.transactionGap = transactionGap;
        this.rand = new Random(seed);
    }

    @Override
    public ShoppingCartRecord apply(Long recordIndex) {
        rand.setSeed(recordIndex * 104_729L);

        ShoppingCartRecord result;

        switch (getAction()) {
            case NEW:
                result = createShoppingCart();
                activeCarts.add(result);
                result.getItems().add(createCartItem(result.getCountry()));
                break;

            case COMPLETED:
                int cartIndex = rand.nextInt(activeCarts.size());
                if (!activeCarts.get(cartIndex).getItems().isEmpty()) {
                    result = activeCarts.remove(cartIndex);
                    result.setTransactionCompleted(true);
                    updateTransactionTime(result);
                    break;
                }

                // No items, drop through to update case

            case UPDATE:
                result = activeCarts.get(rand.nextInt(activeCarts.size()));
                // Add item or change quantity
                float updateAction = rand.nextFloat();
                List<CartItem> items = result.getItems();
                if (items.isEmpty() || (updateAction < 0.50)) {
                    items.add(createCartItem(result.getCountry()));
                } else {
                    // Update quantity. If it's 0, remove it
                    int itemIndex = rand.nextInt(items.size());
                    CartItem item = items.get(itemIndex);
                    if (item.getQuantity() == MAX_PRODUCT_QUANTITY) {
                        item.setQuantity(MAX_PRODUCT_QUANTITY - 1);
                    } else {
                        item.setQuantity(item.getQuantity() + (rand.nextBoolean() ? 1 : -1));
                        if (item.getQuantity() == 0) {
                            items.remove(itemIndex);
                        }
                    }
                }

                updateTransactionTime(result);
                break;

            default:
                throw new RuntimeException("Unknown action");
        }

        return result;
    }

    private void updateTransactionTime(ShoppingCartRecord result) {
        long transTime = result.getTransactionTime();
        transTime += Math.abs(rand.nextGaussian(transactionGap, transactionGap / 5.0));
        result.setTransactionTime(transTime);

        // Advance curTime so we get new records at a reasonable time.
        curTime = Math.max(curTime, transTime);
    }

    @VisibleForTesting
    public ShoppingCartRecord createShoppingCart() {
        ShoppingCartRecord result = new ShoppingCartRecord();
        result.setTransactionTime(curTime);
        // Advance transaction time.
        curTime += Math.abs(rand.nextGaussian(transactionGap, transactionGap / 5.0));

        result.setCustomerId("C" + rand.nextInt(100000));
        result.setTransactionId("T" + rand.nextLong());
        result.setTransactionCompleted(false);

        // Majority are US, followed by Mexico, etc.
        result.setCountry(COUNTRIES[makeExpDecayValue(0, COUNTRIES.length - 1)]);
        result.setTransactionCompleted(false);
        if (rand.nextDouble() < 0.95) {
            // 95% are empty, otherwise 1 of 10 codes
            result.setCouponCode("");
        } else {
            result.setCouponCode(String.format("C%03d", rand.nextInt(10)));
        }

        // Set payment method, mostly CC, else PayPal, etc.
        result.setPaymentMethod(PAYMENT_METHODS[makeExpDecayValue(0, PAYMENT_METHODS.length - 1)]);
        result.setShippingAddress(generateFakeAddress(result.getCustomerId()));
        return result;
    }

    @VisibleForTesting
    public CartItem createCartItem(String country) {
        // Add item
        CartItem newItem = new CartItem();
        // Set quantity, exp decay from 1 to N
        newItem.setQuantity(makeExpDecayValue(1, MAX_PRODUCT_QUANTITY));
        // Set product randomly to 1 of 20
        newItem.setProductId(String.format("P%04d", rand.nextInt(ProductInfoGenerator.NUM_UNIQUE_PRODUCTS)));
        // Set price based on product id & country
        newItem.setPrice(makePrice(newItem.getProductId(), country));
        // Unenriched record doesn't have this data yet
        newItem.setCategory("");
        newItem.setProductName("");
        newItem.setWeightKg(0.0);
        newItem.setUsDollarEquivalent(0.0);

        return newItem;
    }

    private double makePrice(String productId, String country) {
        double priceInUSD = makeExpDecayValue(MIN_PRICE, MAX_PRICE);
        return country.equals("US") ? priceInUSD : priceInUSD / ExchangeRates.STARTING_RATES.get(country);
    }

    @VisibleForTesting
    protected String generateFakeAddress(String customerId) {
        rand.setSeed(customerId.hashCode());

        // Generate street number (1-9999)
        int streetNumber = rand.nextInt(9999) + 1;

        // Select random street name and type
        String streetName = STREET_NAMES[rand.nextInt(STREET_NAMES.length)];
        String streetType = STREET_TYPES[rand.nextInt(STREET_TYPES.length)];

        // Select random city
        String city = CITIES[rand.nextInt(CITIES.length)];

        // Select random state
        String state = STATES[rand.nextInt(STATES.length)];

        // Generate ZIP code (10000-99999)
        int zipCode = rand.nextInt(90000) + 10000;

        // Construct and return the address string
        return String.format("%d %s %s, %s, %s %05d",
                streetNumber, streetName, streetType, city, state, zipCode);
    }

    protected int makeExpDecayValue(int minVal, int maxVal) {
        return (int)makeExpDecayValue((double)minVal, (double)maxVal);
    }

    @VisibleForTesting
    protected double makeExpDecayValue(double minVal, double maxVal) {
        Preconditions.checkArgument(maxVal >= minVal, "Min can't be greater than max");

        // Calculate the range
        double valueRange = maxVal - minVal + 1;

        // Generate a random value with exponential distribution
        double lambda = 4;

        double randomExp = -Math.log(1 - (1 - Math.exp(-lambda)) * rand.nextDouble()) / lambda;
        double scaledValue = (valueRange * randomExp);
        return minVal + scaledValue;
    }

    // 5% of time it's an add, 75% it's an update, and 20% it's a completed
    private CartAction getAction() {
        if (activeCarts.isEmpty()) {
            return CartAction.NEW;
        }

        float actionVal = rand.nextFloat();
        if ((actionVal < 0.05) && (activeCarts.size() < MAX_ACTIVE_CARTS)) {
            return CartAction.NEW;
        }

        if (actionVal < 0.75) {
            return CartAction.UPDATE;
        }

        return CartAction.COMPLETED;
    }


    private enum CartAction {
        NEW,
        UPDATE,
        COMPLETED
    }

}
