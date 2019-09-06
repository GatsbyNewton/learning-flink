package edu.wzm.join.interval

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

abstract class TimeStamp

case class Order(orderId: String, product: String, orderTime: Long) extends TimeStamp

case class Payment(orderId: String, payType: String, payTime: Long) extends TimeStamp

/**
  * 计算下单后一小时之内支付的订单明细。
  *
  * 注意：Flink默认用的是格林威治时间GMT，可用UDF换算成北京时间GMT+8，例子中未做处理。
  */
object SimpleTimeIntervalJoin {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val tEnv = StreamTableEnvironment.create(env, settings)
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val orderData = Seq(Order("001", "iphone", 1545800002000L), // 2018-12-26 12:53:22
                            Order("002", "mac", 1545800003000L),    // 2018-12-26 12:53:23
                            Order("003", "book", 1545800004000L),   // 2018-12-26 12:53:24
                            Order("004", "cup", 1545800018000L))    //2018-12-26 12:53:38

        val paymentData = Seq(Payment("001", "alipay", 1545803501000L), // 2018-12-26 13:51:41
                              Payment("002", "card", 1545803602000L),   // 2018-12-26 13:53:22
                              Payment("003", "card", 1545803610000L),   // 2018-12-26 13:53:30
                              Payment("004", "alipay", 1545803611000L)) // 2018-12-26 13:53:31

        val orderTable = env.fromCollection(orderData)
          .assignTimestampsAndWatermarks(new EventTimeExtractor[Order])
          .toTable(tEnv, 'orderId, 'product, 'orderTime.rowtime)

        val paymentTable = env.fromCollection(paymentData)
          .assignTimestampsAndWatermarks(new EventTimeExtractor[Payment])
          .toTable(tEnv, 'orderId, 'payType, 'payTime.rowtime)

        tEnv.registerTable("orders", orderTable)
        tEnv.registerTable("payment", paymentTable)

        val sql =
            """
              |SELECT
              | o.orderId,
              | o.product,
              | p.payType,
              | o.orderTime,
              | CAST(p.payTime AS TIMESTAMP) AS payTime
              |FROM orders AS o
              |JOIN payment AS p
              |  ON o.orderId = p.orderId AND p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' HOUR
            """.stripMargin
        tEnv.registerTable("TemporalJoinTable", tEnv.sqlQuery(sql))

        val result = tEnv.scan("TemporalJoinTable").toAppendStream[Row]
        result.print()
        env.execute("Interval Join")
    }
}

class EventTimeExtractor[TimeStamp]
  extends BoundedOutOfOrdernessTimestampExtractor[TimeStamp](Time.seconds(10)){
    override def extractTimestamp(t: TimeStamp): Long = t match {
        case o: Order if o.isInstanceOf[Order] => o.orderTime
        case p: Payment if p.isInstanceOf[Payment] => p.payTime
    }

}