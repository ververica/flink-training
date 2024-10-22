package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.KeyedWindowDouble;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@DoNotChangeThis
public class SetKeyAndTimeFunction extends ProcessWindowFunction<Double, KeyedWindowDouble, String, TimeWindow> {
    @Override
    public void process(String key, Context ctx, Iterable<Double> elements, Collector<KeyedWindowDouble> out) throws Exception {
        out.collect(new KeyedWindowDouble(key, ctx.window().getStart(), elements.iterator().next()));
    }
}