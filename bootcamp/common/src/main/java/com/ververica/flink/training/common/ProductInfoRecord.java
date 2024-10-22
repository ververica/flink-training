package com.ververica.flink.training.common;

@DoNotChangeThis
public class ProductInfoRecord {
    private long infoTime;
    private String productId;
    private String productName;
    private String category;
    private double weightKg;

    public ProductInfoRecord() {}

    public long getInfoTime() {
        return infoTime;
    }

    public void setInfoTime(long infoTime) {
        this.infoTime = infoTime;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getWeightKg() {
        return weightKg;
    }

    public void setWeightKg(double weightKg) {
        this.weightKg = weightKg;
    }
}
