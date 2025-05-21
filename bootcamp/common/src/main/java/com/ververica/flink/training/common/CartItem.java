package com.ververica.flink.training.common;

import static com.ververica.flink.training.common.TextParseUtils.getField;
import static com.ververica.flink.training.common.TextParseUtils.getTextField;

@DoNotChangeThis
public class CartItem {
    private String productId;
    private int quantity;
    private double price;
    private double usDollarEquivalent;

    // New fields
    private String productName;
    private String category;
    private double weightKg;

    public CartItem() { }

    public CartItem(CartItem clone) {
        productId = clone.productId;
        quantity = clone.quantity;
        price = clone.price;
        usDollarEquivalent = clone.usDollarEquivalent;
        productName = clone.productName;
        category = clone.category;
        weightKg = clone.weightKg;
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

    public double getUsDollarEquivalent() {
        return usDollarEquivalent;
    }

    public void setUsDollarEquivalent(double usDollarEquivalent) {
        this.usDollarEquivalent = usDollarEquivalent;
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

    @Override
    public String toString() {
        return "CartItem{" +
                "productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", usDollarEquivalent=" + usDollarEquivalent +
                ", productName='" + productName + '\'' +
                ", category='" + category + '\'' +
                ", weightKg=" + weightKg +
                '}';
    }

    public static CartItem fromString(String s) {
        CartItem result = new CartItem();

        result.setProductId(getTextField(s, "productId"));
        result.setQuantity(Integer.parseInt(getField(s, "quantity")));
        result.setPrice(Double.parseDouble(getField(s, "price")));
        result.setUsDollarEquivalent(Double.parseDouble(getField(s, "usDollarEquivalent")));
        result.setProductName(getTextField(s, "productName"));
        result.setCategory(getTextField(s, "category"));
        result.setWeightKg(Double.parseDouble(getField(s, "weightKg")));

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CartItem cartItem = (CartItem) o;

        if (quantity != cartItem.quantity) return false;
        if (Double.compare(cartItem.price, price) != 0) return false;
        if (Double.compare(cartItem.usDollarEquivalent, usDollarEquivalent) != 0) return false;
        if (Double.compare(cartItem.weightKg, weightKg) != 0) return false;
        if (!productId.equals(cartItem.productId)) return false;
        if (!productName.equals(cartItem.productName)) return false;
        return category.equals(cartItem.category);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = productId.hashCode();
        result = 31 * result + quantity;
        temp = Double.doubleToLongBits(price);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(usDollarEquivalent);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + productName.hashCode();
        result = 31 * result + category.hashCode();
        temp = Double.doubleToLongBits(weightKg);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
