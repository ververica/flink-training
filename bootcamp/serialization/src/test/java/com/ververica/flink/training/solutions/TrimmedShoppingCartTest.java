package com.ververica.flink.training.solutions;

import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;

public class TrimmedShoppingCartTest {

    @Test
    public void testSerializableAsPojo() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(TrimmedShoppingCart.class);
    }
}
