package edu.wzm.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/12
 * @version: 1.0
 * @Description: This example shows how to implement LEFT OUTER JOIN with CoGroupFunction
 */
public class LeftOuterJoin {
    private static final Logger LOGGER = LogManager.getLogger(LeftOuterJoin.class);

    public static void main(String[] args) {
        final int port1;
        final int port2;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port1 = params.getInt("port1");
            port2 = params.getInt("port2");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'LeftOuterJoin --por1 <port> --por2 <port>'");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<User> user = JoinSource.getUserStream(env, "localhost", port1);

        DataStream<Order> order = JoinSource.getOrderStream(env, "localhost", port2);

        DataStream<Detail> result = user.coGroup(order)
                .where(User::getUserId)
                .equalTo(Order::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .apply(new LeftOuterJoinCoGroupFunction());

        result.print();

        try {
            env.execute("LEFT OUTER JOIN");
        } catch (Exception e) {
            LOGGER.error("Job Failed!", e);
        }
    }

    private static class LeftOuterJoinCoGroupFunction implements CoGroupFunction<User, Order, Detail> {

        @Override
        public void coGroup(Iterable<User> users, Iterable<Order> orders, Collector<Detail> collector) throws Exception {
            for (User user : users) {
                boolean isBuy = false;
                for (Order order : orders) {
                    collector.collect(new Detail(user.getUsername(), order.getOrderId(), order.getTimestamp()));
                    isBuy = true;
                }

                if (!isBuy) {
                    collector.collect(new Detail(user.getUsername(), null, 0L));
                }
            }
        }
    }
}
