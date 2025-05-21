package com.ververica.flink.training.common;

import java.util.HashMap;
import java.util.Map;

@DoNotChangeThis
public interface ExchangeRates {

    public static final Map<String, Double> STARTING_RATES = new HashMap<String, Double>() {{
        put("JP", 0.0070);
        put("CN", 0.14);
        put("CA", 0.74);
        put("MX", 0.051);
    }};

}
