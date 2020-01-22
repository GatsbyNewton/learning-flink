package edu.wzm.external;

import edu.wzm.kafka.Event;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/22
 * @version: 1.0
 * @Description: Access external system with Async IO
 *
 * <p> Example of accessing MySQL with Async IO
 *     MySQL table:
 *          CREATE TABLE user (
 *            user_id int(11) NOT NULL DEFAULT 0 AUTO_INCREMENT,
 *            username varchar(16) DEFAULT NULL,
 *            PRIMARY KEY (user_id)
 *          )
 * )
 */
public class MySQLWithAsyncIO {
    private static final Logger LOGGER = LogManager.getLogger(MySQLWithAsyncIO.class);

    private static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/test";
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "12345678";

    public static void main(String... args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String topic;
        if (!params.has("topic")) {
            System.err.println("The Parameter is incorrect. " +
                    "Usage: MySQLWithAsyncIO --topic <topic>");
            return;
        } else {
            topic = params.get("topic");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        FlinkKafkaConsumer010<Order> kafka = new FlinkKafkaConsumer010<>(topic, new KafkaSchema(), props);

        DataStream<Order> orderDS = env.addSource(kafka.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("userId");

        DataStream<Detail> result = connectExternalSys(orderDS);

        result.print("External System ");

        try {
            env.execute("External System With Async I/O");
        } catch (Exception e) {
            LOGGER.error("Job Failed!", e);
        }
    }

    public static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<Order> {

        private long currentTimestamp = 0L;
        private final Long maxOutOfOrderness = 1000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Order order, long l) {
            currentTimestamp = order.getTimestamp();
            return order.getTimestamp();
        }
    }

    private static DataStream<Detail> connectExternalSys(DataStream<Order> orderDS) {
        return AsyncDataStream.orderedWait(orderDS, new RichAsyncFunction<Order, Detail>() {
            private Connection conn;
            private PreparedStatement stmt;
            private ResultSet rs;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName(DB_DRIVER);
                conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
                conn.setAutoCommit(false);
            }

            @Override
            public void asyncInvoke(Order order, ResultFuture<Detail> resultFuture) throws Exception {
                stmt = conn.prepareStatement("SELECT * FROM user WHERE user_id = ?");
                stmt.setString(1, order.getUserId());
                rs = stmt.executeQuery();

                List<Detail> details = new ArrayList<>(16);
                while (rs != null && rs.next()) {
                    details.add(
                            new Detail(
                                    order.getOrderId(),
                                    rs.getString("user_id"),
                                    rs.getString("username"),
                                    order.getTimestamp()));
                }

                resultFuture.complete(details);
            }

            @Override
            public void close() throws Exception {
                super.close();
                if (rs != null && !rs.isClosed()) {
                    rs.close();
                }

                if (stmt != null && !stmt.isClosed()) {
                    stmt.close();
                }

                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
        }, 5000, TimeUnit.SECONDS, 100);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Detail {
        private String orderId;
        private String userId;
        private String username;
        private Long timestamp;
    }
}
