package edu.wzm.topk;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopKHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
    private static final Logger LOGGER = LogManager.getLogger(TopKHotItems.class);

    private final int hotK;

    private ListState<ItemViewCount> state;

    public TopKHotItems(int hotK) {
        this.hotK = hotK;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor stateDescriptor = new ListStateDescriptor("hot-item-state", ItemViewCount.class);
        state = getRuntimeContext().getListState(stateDescriptor);
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        state.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEndTimeStamp() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> allItem = new ArrayList<>();
        for (ItemViewCount viewCount : state.get()){
            allItem.add(viewCount);
        }

        state.clear();
        allItem.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.getClickCount() - o1.getClickCount());
            }
        });

        StringBuffer sb = new StringBuffer();
        sb.append("--------------------------------------\n");
        for (int i = 0; i < allItem.size() && i < hotK; i++){
                sb.append("No").append(i + 1).append(":")
                        .append("  商品ID=").append(allItem.get(i).getItemId())
                        .append("  浏览量=").append(allItem.get(i).getClickCount())
                        .append("\n");
        }
        sb.append("--------------------------------------\n\n");

        out.collect(sb.toString());
    }
}
