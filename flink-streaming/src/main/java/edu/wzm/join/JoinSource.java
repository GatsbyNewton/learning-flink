package edu.wzm.join;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: wangzhiming
 * @Date: 2020/1/12
 * @version:
 * @Description:
 */
public class JoinSource {
    private static final String ENTER_SEPARATOR = "\n";
    private static final String SPACE_SEPARATOR = "\\s+";

    public static DataStream<User> getUserStream(StreamExecutionEnvironment env, String ip, int port) {
        return env.socketTextStream(ip, port, ENTER_SEPARATOR)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.trim().split(SPACE_SEPARATOR)[2]);
                    }
                })
                .map(str -> {
                    String[] msg = str.trim().split(SPACE_SEPARATOR);
                    return new User(msg[0], msg[1], Long.valueOf(msg[2]));
                });
    }

    public static DataStream<Order> getOrderStream(StreamExecutionEnvironment env, String ip, int port) {
        return env.socketTextStream(ip, port, ENTER_SEPARATOR)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.trim().split(SPACE_SEPARATOR)[2]);
                    }
                })
                .map(str -> {
                    String[] msg = str.trim().split(SPACE_SEPARATOR);
                    return new Order(msg[0], msg[1], Long.valueOf(msg[2]));
                });
    }
}
