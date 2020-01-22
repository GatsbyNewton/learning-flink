package edu.wzm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/20
 * @version: 1.0
 * @Description: Dynamic Table join Dimension Table
 *
 * <p> Example of stream table join MySQL static table.
 */
public class DimensionTable {
    private static final Logger LOGGER = LogManager.getLogger(DimensionTable.class);

    private static final String SPACE_SEPARATOR = "\\s+";

    public static void main(String[] args) {
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'LeftOuterJoin --port <port>'");
            return;
        }

        TypeInformation<?>[] typeInformation = new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformation);

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/test")
                .setUsername("root")
                .setPassword("12345678")
                .setQuery("SELECT * FROM user")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Order> stream = env.socketTextStream("localhost", port)
                .map(str -> {
                    String[] msg = str.trim().split(SPACE_SEPARATOR);
                    return new Order(msg[0], Integer.valueOf(msg[1]), Long.valueOf(msg[2]));
                });

        DataStream<Row> dimension = env.createInput(jdbcInputFormat);

        Table streamTable = tEnv.fromDataStream(stream);
        Table dimensionTable = tEnv.fromDataStream(dimension, "id, username");
        Table result = streamTable.join(dimensionTable)
                .where("userId = id")
                .select("orderId, userId, username, opTime");
        tEnv.toAppendStream(result, Detail.class).print();

        try {
            env.execute("Dimension Join");
        } catch (Exception e) {
            LOGGER.error("Job Failed!", e);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Long opTime;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Detail {
        private String orderId;
        private Integer userId;
        private String username;
        private Long opTime;
    }
}
