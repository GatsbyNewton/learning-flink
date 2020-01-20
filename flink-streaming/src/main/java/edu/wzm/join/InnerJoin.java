package edu.wzm.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/12
 * @version: 1.0
 * @Description: This example shows how to implement INNER JOIN with JoinFunction
 */
public class InnerJoin {
    private static final Logger LOGGER = LogManager.getLogger(InnerJoin.class);

    public static void main(String[] args) {
        final int port1;
        final int port2;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port1 = params.getInt("port1");
            port2 = params.getInt("port2");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'InnerJoin --por1 <port> --por2 <port>'");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<User> user = JoinSource.getUserStream(env, "localhost", port1);

        DataStream<Order> order = JoinSource.getOrderStream(env, "localhost", port2);

        DataStream<Detail> result = user.join(order)
                .where(User::getUserId)
                .equalTo(Order::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .apply(new InnerJoinFunction());

        result.print().setParallelism(1);

        try {
            env.execute("Inner Join");
        } catch (Exception e) {
            LOGGER.error("Job Failed!", e);
        }
    }

    private static class InnerJoinFunction implements JoinFunction<User, Order, Detail> {
        @Override
        public Detail join(User user, Order order) throws Exception {
            return new Detail(user.getUsername(), order.getOrderId(), order.getTimestamp());
        }
    }
}
