package com.ververica.flink.training.common;

/*
 * Class to save result from a .windowAll calculation, where there is no key,
 * just a window start time and a result.
 */
@DoNotChangeThis
public class WindowAllResult {

    private long time;
    private long result;

    public WindowAllResult() {}

    public WindowAllResult(long time, long result) {
        this.time = time;
        this.result = result;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getResult() {
        return result;
    }

    public void setResult(long result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "WindowAllResult{" +
                "time=" + time +
                ", result=" + result +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WindowAllResult that = (WindowAllResult) o;

        if (time != that.time) return false;
        return result == that.result;
    }

    @Override
    public int hashCode() {
        int result1 = (int) (time ^ (time >>> 32));
        result1 = 31 * result1 + (int) (result ^ (result >>> 32));
        return result1;
    }
}
