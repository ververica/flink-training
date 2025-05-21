package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.KeyedWindowResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * A ProcessWindowFunction that adds the key & window start time to the result.
 */
public class SetKeyAndTimeFunction extends ProcessWindowFunction<Long, KeyedWindowResult, String, TimeWindow> {
    @Override
    public void process(String key, Context ctx, Iterable<Long> elements, Collector<KeyedWindowResult> out) throws Exception {
        out.collect(new KeyedWindowResult(key, ctx.window().getStart(), elements.iterator().next()));
    }
}
