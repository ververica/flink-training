package com.ververica.flink.training.common;

/**
 * Generic result type that has a key, a window start time, and a (double) value for
 * the result.
 */
@DoNotChangeThis
public class KeyedWindowDouble {
    private String key;
    private long time;
    private double result;

    public KeyedWindowDouble() {}

    public KeyedWindowDouble(String key, long time, double result) {
        this.key = key;
        this.time = time;
        this.result = result;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getResult() {
        return result;
    }

    public void setResult(double result) {
        this.result = result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyedWindowDouble that = (KeyedWindowDouble) o;

        if (time != that.time) return false;
        if (Double.compare(that.result, result) != 0) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result1;
        long temp;
        result1 = key.hashCode();
        result1 = 31 * result1 + (int) (time ^ (time >>> 32));
        temp = Double.doubleToLongBits(result);
        result1 = 31 * result1 + (int) (temp ^ (temp >>> 32));
        return result1;
    }

    @Override
    public String toString() {
        return "KeyedWindowDouble{" +
                "key='" + key + '\'' +
                ", time=" + time +
                ", result=" + result +
                '}';
    }
}
