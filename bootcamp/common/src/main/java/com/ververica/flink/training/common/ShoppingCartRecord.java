package com.ververica.flink.training.common;

import org.apache.flink.api.common.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ververica.flink.training.common.TextParseUtils.getField;
import static com.ververica.flink.training.common.TextParseUtils.getTextField;

/**
 * A typical eCommerce shopping cart.
 */
public class ShoppingCartRecord {
    private String transactionId;
    private String country;
    private boolean transactionCompleted;
    private long transactionTime;
    private String customerId;
    private String paymentMethod;
    private String shippingAddress;
    private double shippingCost;
    private String couponCode;
    // TODO - you might want to use the @TypeInfo annotation to
    // improve serialization performance.
    private List<CartItem> items;

    public ShoppingCartRecord() {
        setItems(new ArrayList<>());
    }

    public ShoppingCartRecord(ShoppingCartRecord clone) {
        setTransactionId(clone.getTransactionId());
        setCountry(clone.getCountry());
        setTransactionCompleted(clone.isTransactionCompleted());
        setTransactionTime(clone.getTransactionTime());
        setCustomerId(clone.getCustomerId());
        setPaymentMethod(clone.getPaymentMethod());
        setShippingAddress(clone.getShippingAddress());
        setShippingCost(clone.getShippingCost());
        setCouponCode(clone.getCouponCode());

        setItems(new ArrayList<>());
        for (CartItem item : clone.getItems()) {
            getItems().add(new CartItem(item));
        }
    }

    public List<CartItem> getItems() {
        return items;
    }

    public void setItems(List<CartItem> items) {
        this.items = items;
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

    public boolean isTransactionCompleted() {
        return transactionCompleted;
    }

    public void setTransactionCompleted(boolean transactionCompleted) {
        this.transactionCompleted = transactionCompleted;
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

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(String shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public double getShippingCost() {
        return shippingCost;
    }

    public void setShippingCost(double shippingCost) {
        this.shippingCost = shippingCost;
    }

    public String getCouponCode() {
        return couponCode;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

    @Override
    public String toString() {
        return "ShoppingCartRecord{" +
                "transactionId='" + transactionId + '\'' +
                ", country='" + country + '\'' +
                ", transactionCompleted=" + transactionCompleted +
                ", transactionTime=" + transactionTime +
                ", customerId='" + customerId + '\'' +
                ", paymentMethod='" + paymentMethod + '\'' +
                ", shippingAddress='" + shippingAddress + '\'' +
                ", shippingCost=" + shippingCost +
                ", couponCode='" + couponCode + '\'' +
                ", items=" + items +
                '}';
    }

    public static ShoppingCartRecord fromString(String s) {
        ShoppingCartRecord result = new ShoppingCartRecord();

        result.setTransactionId(getTextField(s, "transactionId"));
        result.setCountry(getTextField(s, "country"));
        result.setTransactionCompleted(Boolean.parseBoolean(getField(s, "transactionCompleted")));
        result.setTransactionTime(Long.parseLong(getField(s, "transactionTime")));
        result.setCustomerId(getTextField(s, "customerId"));
        result.setPaymentMethod(getTextField(s, "paymentMethod"));
        result.setShippingAddress(getTextField(s, "shippingAddress"));
        result.setShippingCost(Double.parseDouble(getField(s, "shippingCost")));
        result.setCouponCode(getTextField(s, "couponCode"));
        result.setItems(getItemsFromString(s));
        return result;
    }

    private static List<CartItem> getItemsFromString(String s) {
        List<CartItem> result = new ArrayList<>();

        Pattern itemsPattern = Pattern.compile("items=\\[(.*)]}");
        Matcher itemsMatcher = itemsPattern.matcher(s);
        if (!itemsMatcher.find()) {
            throw new RuntimeException("Can't find items field in " + s);
        }

        String itemsString = itemsMatcher.group(1);
        if (itemsString.isEmpty()) {
            return result;
        }

        Pattern p = Pattern.compile("CartItem\\{(.+?)\\}(, |$)");
        Matcher m = p.matcher(itemsString);
        while (m.find()) {
            CartItem item = CartItem.fromString(m.group(1));
            result.add(item);
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShoppingCartRecord that = (ShoppingCartRecord) o;

        if (transactionCompleted != that.transactionCompleted) return false;
        if (transactionTime != that.transactionTime) return false;
        if (Double.compare(that.shippingCost, shippingCost) != 0) return false;
        if (!transactionId.equals(that.transactionId)) return false;
        if (!country.equals(that.country)) return false;
        if (!customerId.equals(that.customerId)) return false;
        if (!paymentMethod.equals(that.paymentMethod)) return false;
        if (!shippingAddress.equals(that.shippingAddress)) return false;
        if (!couponCode.equals(that.couponCode)) return false;
        return items.equals(that.items);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = transactionId.hashCode();
        result = 31 * result + country.hashCode();
        result = 31 * result + (transactionCompleted ? 1 : 0);
        result = 31 * result + (int) (transactionTime ^ (transactionTime >>> 32));
        result = 31 * result + customerId.hashCode();
        result = 31 * result + paymentMethod.hashCode();
        result = 31 * result + shippingAddress.hashCode();
        temp = Double.doubleToLongBits(shippingCost);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + couponCode.hashCode();
        result = 31 * result + items.hashCode();
        return result;
    }
}
