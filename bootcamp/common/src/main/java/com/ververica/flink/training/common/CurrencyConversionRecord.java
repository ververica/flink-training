package com.ververica.flink.training.common;

import java.util.Date;

@DoNotChangeThis
public class CurrencyConversionRecord {
    private String exchangeRateCountry;
    private Date exchangeRateTime;
    private double exchangeRate;

    public CurrencyConversionRecord() { }

    public CurrencyConversionRecord(String country, Date timestamp, double conversionRate) {
        this.exchangeRateCountry = country;
        this.exchangeRateTime = timestamp;
        this.exchangeRate = conversionRate;
    }

    // Getters
    public String getExchangeRateCountry() { return exchangeRateCountry; }
    public Date getExchangeRateTime() { return exchangeRateTime; }
    public double getExchangeRate() { return exchangeRate; }

    public void setExchangeRateCountry(String exchangeRateCountry) {
        this.exchangeRateCountry = exchangeRateCountry;
    }

    public void setExchangeRateTime(Date exchangeRateTime) {
        this.exchangeRateTime = exchangeRateTime;
    }

    public void setExchangeRate(double exchangeRate) {
        this.exchangeRate = exchangeRate;
    }
}
