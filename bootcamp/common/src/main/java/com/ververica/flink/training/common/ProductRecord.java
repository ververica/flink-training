package com.ververica.flink.training.common;

@DoNotChangeThis
public class ProductRecord {
    // Fields from ShoppingCartRecord that we need
    private String transactionId;
    private String country;
    private long transactionTime;
    private String customerId;

    // Fields from CartItem that we need
    private String productId;
    private int quantity;
    private double price;

    // New fields, following enrichment
    private String productName;
    private String category;
    private double weightKg;

    public ProductRecord() {}

    public ProductRecord(ShoppingCartRecord parent, CartItem item) {
        this.transactionId = parent.getTransactionId();
        this.country = parent.getCountry();
        this.transactionTime = parent.getTransactionTime();
        this.customerId = parent.getCustomerId();

        this.productId = item.getProductId();
        this.quantity = item.getQuantity();
        this.price = item.getPrice();

        // Unenriched values.
        this.productName = "";
        this.category = "";
        this.weightKg = 0.0;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
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
