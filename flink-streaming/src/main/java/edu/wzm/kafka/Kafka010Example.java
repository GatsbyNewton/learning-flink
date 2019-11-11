package edu.wzm.kafka;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

/**
 * Simple example for demonstrating the use of Kafka and Flink Window.
 *
 * <p>Usage: Kafka010Example --input-topic &lt;topic&gt; --output-topic &lt;topic&gt; --path &lt;checkpoint path&gt; --type &lt;window|no&gt;
 *     eg. Kafka010Example --input-topic &lt;input&gt; --output-topic &lt;output&gt; --path &lt;file:///Users/jimmy/flink&gt; --type &lt;window|no&gt;
 */
public class Kafka010Example {

    private static final Logger LOGGER = LogManager.getLogger(Kafka010Example.class);

    public static void main(String... args) {
        final ParameterTool param = ParameterTool.fromArgs(args);
        if (!param.has("input-topic")
                || !param.has("output-topic")
                || !param.has("path")
                || !param.has("type")) {
            System.out.println("The Parameters is incorrect. " +
                    "Usage: Kafka010Example --input-topic <topic> --output-topic <topic> --path <checkpoint path> --type <window|none>");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(param);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // set checkpoint config
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // set state backend
        env.setStateBackend(new FsStateBackend(param.get("path")));

        FlinkKafkaConsumer010<Event> kafkaSource = new FlinkKafkaConsumer010<>(
                param.get("input-topic"),
                new KafkaSchema(),
                getConsumerProperties());

        // consume Kafka message from beginning
        kafkaSource.setStartFromEarliest();

        if (param.get("type").equals("window")) {
            runWitWindow(env, kafkaSource, param);
        } else {
            runWithoutWindow(env, kafkaSource);
        }
    }

    private static void runWithoutWindow(StreamExecutionEnvironment env,
                                         FlinkKafkaConsumer010<Event> kafkaSource) {
        DataStream<Event> data = env
                // Timestamp assignment and watermark generation are usually
                // applied as close to a source operator as possible.
                .addSource(kafkaSource.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("word")
                .map(new TotalMapFunction());

        data.print();

        try {
            env.execute("Kafka Example WithoutWindow");
        } catch (Exception e) {
            LOGGER.error("The job failed.", e);
        }
    }

    private static void runWitWindow(StreamExecutionEnvironment env,
                                     FlinkKafkaConsumer010<Event> kafkaSource,
                                     ParameterTool param) {
        DataStream<Tuple2<String, Integer>> data = env
                // Timestamp assignment and watermark generation are usually
                // applied as close to a source operator as possible.
                .addSource(kafkaSource.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("word")
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new ProcessWindowFunction<Event, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    MapState<String, Integer> countMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> mapDesc = new MapStateDescriptor<>("count", String.class, Integer.class);
                        countMap = getRuntimeContext().getMapState(mapDesc);
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Event> iterable, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (Event event : iterable) {
                            if (countMap.contains(event.getWord())) {
                                countMap.put(event.getWord(), countMap.get(event.getWord()) + event.getFrequency());
                            } else {
                                countMap.put(event.getWord(), event.getFrequency());
                            }

                        }

                        for (Map.Entry<String, Integer> entry : countMap.entries()) {
                            out.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
                        }
                    }
                });


        data.addSink(
                new FlinkKafkaProducer010<>(
                        param.get("output-topic"),
                        new KafkaSchema(),
                        getProducerProperties()));

        try {
            env.execute("Kafka Example WithWindow");
        } catch (Exception e) {
            LOGGER.error("The job failed.", e);
        }
    }

    private static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        return props;
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("batch.size", "1");
        return props;
    }


    public static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<Event> {

        private long currentTimestamp = 0L;
        private final Long maxOutOfOrderness = 1000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Event event, long l) {
            currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }
    }

    public static class TotalMapFunction extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1180234853172462378L;

        private transient ValueState<Integer> currentTotalCount;

        @Override
        public Event map(Event event) throws Exception {
            Integer totalCount = currentTotalCount.value();

            if (totalCount == null) {
                totalCount = 0;
            }
            totalCount += event.getFrequency();

            currentTotalCount.update(totalCount);

            return new Event(event.getWord(), totalCount, event.getTimestamp());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
        }
    }
}
