package edu.wzm.topk;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, java.lang.Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
        Long itemId = ((Tuple1<Long>)tuple).f0;
        Long count = iterable.iterator().next();
        collector.collect(ItemViewCount.of(itemId, timeWindow.getEnd(), count));
    }
}
