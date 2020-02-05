package edu.wzm.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * @author: wangzhiming
 * @Date: 2020/2/5
 * @version:
 * @Description:
 *      Test Dataset:
 *          bill 10  1577259310000
 *          alice 20 1577259313000
 *          bill 20  1577259312000
 *          tom 50   1577259314000
 *          bill 80  1577259320000
 */
public class IncrAndScaleComputation {
    private static final Logger LOGGER = LogManager.getLogger(IncrAndScaleComputation.class);

    private static final String SEPARATOR = "\\s+";
    private static final String EMPTY_STRING = "";

    public static void main(String... args) {
        final int port;
        final String hasTrigger;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            hasTrigger = Objects.equals(params.get("trigger"), null) ? "N" : params.get("trigger");
        } catch (Exception e) {
            System.err.println("No port specified. Please run " +
                    "'IncrAndScaleComputation --port <port> --trigger <y|n>'");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        WindowedStream<Event, String, TimeWindow> stream = text
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split(SEPARATOR)[2]);
                    }
                })
                .map(value -> {
                    String[] vals = value.split(SEPARATOR);
                    return new Event(vals[0], Float.valueOf(vals[1]), Long.valueOf(vals[2]));
                })
                .keyBy(Event::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        if (Objects.equals(hasTrigger.toUpperCase(), "Y")) {
            stream = stream.trigger(CountTrigger.of(1));
        }

        incrAndProc(stream);

        incrBeforeProc(stream);

        try {
            env.execute("Incremental and Scale");
        } catch (Exception e) {
            LOGGER.error("Job Failed", e);
        }
    }

    private static void incrAndProc(WindowedStream<Event, String, TimeWindow> stream) {
        DataStream<Avg> result = stream.aggregate(new AvgAggregateFunction(), new DeduplicatedProcess())
                .uid("incrproc")
                .name("incrproc");

        result.print("incrproc").setParallelism(1);
    }

    private static void incrBeforeProc(WindowedStream<Event, String, TimeWindow> stream) {
        DataStream<Avg> avgStream = stream.aggregate(new AvgAggregateFunction())
                .uid("avg")
                .name("avg");

        avgStream.print("avg:").setParallelism(1);

        avgStream.keyBy(Avg::getUserId)
                .process(new DeduplicatedFunction())
                .uid("process")
                .name("process")
                .print("process:")
                .setParallelism(1);
    }

    private static class AvgAggregateFunction implements AggregateFunction<Event, AvgAggregateACC, Avg> {
        @Override
        public AvgAggregateACC createAccumulator() {
            return new AvgAggregateACC(EMPTY_STRING, 0F, 0);
        }

        @Override
        public AvgAggregateACC add(Event value, AvgAggregateACC acc) {
            if (Objects.equals(acc.getUserId(), EMPTY_STRING)) {
                acc.setUserId(value.getUserId());
            }

            acc.setAmount(acc.getAmount() + value.getPrice());
            acc.setDealCnt(acc.getDealCnt() + 1);

            return acc;
        }

        @Override
        public Avg getResult(AvgAggregateACC acc) {
            return new Avg(acc.getUserId(), acc.getAmount() / acc.getDealCnt());
        }

        @Override
        public AvgAggregateACC merge(AvgAggregateACC acc1, AvgAggregateACC acc2) {
            acc1.setAmount(acc1.getAmount() + acc2.getAmount());
            acc1.setDealCnt(acc1.getDealCnt() + acc2.getDealCnt());

            return acc1;
        }
    }

    private static class DeduplicatedProcess extends ProcessWindowFunction<Avg, Avg, String, TimeWindow> {
        private MapState<String, Avg> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getMapState(new MapStateDescriptor<>("avg", String.class, Avg.class));
        }

        @Override
        public void process(String key, Context context, Iterable<Avg> iterable, Collector<Avg> collector) throws Exception {
            Avg avg = iterable.iterator().next();
            Avg cacheAvg = state.get(avg.getUserId());
            if (Objects.equals(cacheAvg, null) || cacheAvg.getPriceAvg() != avg.getPriceAvg()) {
                Avg res = new Avg();
                res.setUserId(avg.getUserId());
                res.setPriceAvg(avg.getPriceAvg());

                state.put(avg.getUserId(), res);
                collector.collect(res);
            }
        }

        @Override
        public void close() throws Exception {
            state.clear();
        }
    }

    private static class DeduplicatedFunction extends KeyedProcessFunction<String, Avg, Avg> {
        private MapState<String, Avg> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getMapState(new MapStateDescriptor<>("avg", String.class, Avg.class));
        }

        @Override
        public void processElement(Avg avg, Context context, Collector<Avg> collector) throws Exception {
            Avg cacheAvg = state.get(avg.getUserId());
            if (Objects.equals(cacheAvg, null) || cacheAvg.getPriceAvg() != avg.getPriceAvg()) {
                Avg res = new Avg();
                res.setUserId(avg.getUserId());
                res.setPriceAvg(avg.getPriceAvg());

                state.put(avg.getUserId(), res);
                collector.collect(res);
            }
        }

        @Override
        public void close() throws Exception {
            state.clear();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Event {
        private String userId;
        private Float price;
        private Long timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class AvgAggregateACC {
        private String userId;
        private Float amount;
        private Integer dealCnt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Avg {
        private String userId;
        private Float priceAvg;
    }
}
