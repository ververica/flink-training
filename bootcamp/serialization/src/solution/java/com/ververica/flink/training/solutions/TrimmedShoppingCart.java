package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.CartItem;
import com.ververica.flink.training.common.ListInfoFactory;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * A version of ShoppingCartRecord that only has the fields we use, and
 * has a list serializer so that Kryo isn't needed to serialize the items.
 */
public class TrimmedShoppingCart {
    private String transactionId;
    private String country;
    private boolean transactionCompleted;
    private long transactionTime;
    @TypeInfo(ListInfoFactory.class)
    private List<CartItem> items;

    public TrimmedShoppingCart() { }

    public TrimmedShoppingCart(ShoppingCartRecord base) {
        setCountry(base.getCountry());
        setTransactionCompleted(base.isTransactionCompleted());
        setTransactionTime(base.getTransactionTime());
        setTransactionId(base.getTransactionId());

        items = new ArrayList<>();
        List<CartItem> baseItems = base.getItems();
        for (CartItem item : base.getItems()) {
            items.add(new CartItem(item));
        }
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

    public List<CartItem> getItems() {
        return items;
    }

    public void setItems(List<CartItem> items) {
        this.items = items;
    }

}
