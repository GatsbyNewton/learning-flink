package edu.wzm.pvuv;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: Jimmy Wong
 * @Date: 2019-12-26
 * @version: 1.0
 * @Description: Example of PV and UV
 */
public class PvUvExample {

    private static final String SEPARATOR = "\\s+";

    public static void main(String[] args) throws Exception {
        // the port to connect to
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<PvUv> PvUvDataStream = text
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
                @Override
                public long extractTimestamp(String s) {
                    return Long.parseLong(s.split(SEPARATOR)[2]);
                }
            })
            .map(value -> {
                String[] vals = value.split(SEPARATOR);
                return new Event(vals[0], vals[1], Long.parseLong(vals[2]));
            })
            .keyBy("date")
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .trigger(CountTrigger.of(1))
            .aggregate(new PvUvAggregator())
            .map(acc -> PvUv.of(acc.getDate(), acc.getPv(), acc.getUv()));

        // print the results with a single thread, rather than in parallel
        PvUvDataStream.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    private static class PvUvAggregator implements AggregateFunction<Event, PvUvAccumulator, PvUv> {
        @Override
        public PvUvAccumulator createAccumulator() {
            return PvUvAccumulator.of(0L, 0L);
        }

        @Override
        public PvUvAccumulator add(Event value, PvUvAccumulator accumulator) {
            if (!accumulator.getIds().contains(value.getEventId())) {
                accumulator.getIds().add(value.getEventId());
                accumulator.setUv(accumulator.getUv() + 1);
                accumulator.setDate(value.getDate());
            }

            accumulator.setPv(accumulator.getPv() + 1);
            return accumulator;
        }

        @Override
        public PvUv getResult(PvUvAccumulator accumulator) {
            return PvUv.of(accumulator.getDate(), accumulator.getPv(), accumulator.getUv());
        }

        @Override
        public PvUvAccumulator merge(PvUvAccumulator acc1, PvUvAccumulator acc2) {
            acc1.getIds().addAll(acc2.getIds());
            return PvUvAccumulator.of(acc1.getDate(),
                    acc1.getPv() + acc2.getPv(),
                    acc1.getUv() + acc2.getUv(),
                    acc1.getIds());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Event {
        private String date;
        private String eventId;
        private long timestamp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class PvUv {
        private String date;
        private long pv;
        private long uv;

        public static PvUv of(String date, long pv, long uv) {
            return new PvUv(date, pv, uv);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class PvUvAccumulator {
        private String date;
        private long pv;
        private long uv;
        private Set<String> ids;

        public static PvUvAccumulator of(long pv, long uv) {
            return new PvUvAccumulator("", pv, uv, new HashSet<>());
        }

        public static PvUvAccumulator of(String date, long pv, long uv, Set<String> eventIds) {
            return new PvUvAccumulator(date, pv, uv, eventIds);
        }
    }
}