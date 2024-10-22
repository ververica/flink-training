package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.WindowAllResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessAllWindowFunction that generates the result with the window start time.
 */
@DoNotChangeThis
public class SetTimeFunction extends ProcessAllWindowFunction<Long, WindowAllResult, TimeWindow> {

    @Override
    public void process(Context ctx, Iterable<Long> elements, Collector<WindowAllResult> out) throws Exception {
        out.collect(new WindowAllResult(ctx.window().getStart(), elements.iterator().next()));
    }
}
