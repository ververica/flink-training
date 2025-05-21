package com.ververica.flink.training.common;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@DoNotChangeThis
public class ProductInfoGenerator implements SerializableFunction<Long, ProductInfoRecord> {

    public static final int NUM_UNIQUE_PRODUCTS = 20;

    private static final String[] ADJECTIVES = {
            "Super", "Ultra", "Mega", "Quantum", "Turbo", "Hyper", "Eco", "Smart", "Pro", "Elite",
            "Advanced", "Deluxe", "Premium", "Extreme", "Dynamic", "Flex", "Rapid", "Stellar", "Cosmic", "Nano"
    };

    private static final String[] NOUNS = {
            "Blaster", "Fusion", "Wave", "Surge", "Force", "Boost", "Shield", "Pulse", "Core", "Link",
            "Nexus", "Matrix", "Vortex", "Sphere", "Cube", "Prism", "Beacon", "Node", "Flux", "Spark"
    };

    private static final String[] CATEGORIES = {
            "3000", "X", "Plus", "Max", "Elite", "Pro", "Ultra", "Extreme", "Prime", "Omega",
            "Alpha", "Zero", "One", "Neo", "Zen", "Eco", "Flex", "Lite", "Compact", "Portable"
    };

    private Map<String, ProductInfoRecord> productInfos;

    public ProductInfoGenerator() {
        Random rand = new Random(666);
        productInfos = new HashMap<>();

        for (int i = 0; i < NUM_UNIQUE_PRODUCTS; i++) {
            productInfos.put(makeProductId(i), makeProductInfo(rand, i));
        }
    }

    @VisibleForTesting
    public double getProductWeight(String productId) {
        return productInfos.get(productId).getWeightKg();
    }

    private ProductInfoRecord makeProductInfo(Random rand, int productIndex) {
        ProductInfoRecord result = new ProductInfoRecord();
        result.setInfoTime(0L);
        result.setProductId(makeProductId(productIndex));
        String category = CATEGORIES[rand.nextInt(CATEGORIES.length)];
        result.setCategory(category);
        result.setProductName(generateProductName(rand, category));
        result.setWeightKg(rand.nextDouble() * 100.0);
        return result;
    }


    /**
     * Generates a fake product name.
     *
     * @return A string containing a randomly generated product name.
     */
    public static String generateProductName(Random rand, String category) {
        String adjective = ADJECTIVES[rand.nextInt(ADJECTIVES.length)];
        String noun = NOUNS[rand.nextInt(NOUNS.length)];

        // Randomly decide whether to include the category
        boolean includeCategory = rand.nextBoolean();

        if (includeCategory) {
            return String.format("%s %s %s", adjective, noun, category);
        } else {
            return String.format("%s %s", adjective, noun);
        }
    }

    public static String makeProductId(int productIndex) {
        return String.format("P%04d", productIndex);
    }

    @Override
    public ProductInfoRecord apply(Long productIndex) {
        Preconditions.checkNotNull(productIndex);
        Preconditions.checkArgument(productIndex >= 0);

        if (productIndex >= NUM_UNIQUE_PRODUCTS) {
            return null;
        }

        return productInfos.get(makeProductId(productIndex.intValue()));
    }
}
