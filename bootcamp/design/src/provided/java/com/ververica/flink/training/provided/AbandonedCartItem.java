package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;

import static com.ververica.flink.training.common.TextParseUtils.getField;
import static com.ververica.flink.training.common.TextParseUtils.getTextField;

@DoNotChangeThis
public class AbandonedCartItem {
    private String transactionId;
    private long transactionTime;
    private String customerId;
    private String productId;

    public AbandonedCartItem() {}

    public AbandonedCartItem(String transactionId, long transactionTime, String customerId, String productId) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTime = transactionTime;
        this.productId = productId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    @Override
    public String toString() {
        return "AbandonedCartItem{" +
                "transactionId='" + transactionId + '\'' +
                ", transactionTime=" + transactionTime +
                ", customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                '}';
    }

    public static AbandonedCartItem fromString(String s) {
        AbandonedCartItem result = new AbandonedCartItem();

        result.setTransactionId(getTextField(s, "transactionId"));
        result.setTransactionTime(Long.parseLong(getField(s, "transactionTime")));
        result.setCustomerId(getTextField(s, "customerId"));
        result.setProductId(getTextField(s, "productId"));
        return result;

    }
}
