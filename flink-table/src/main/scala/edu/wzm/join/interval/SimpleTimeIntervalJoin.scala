package edu.wzm.join.interval

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

abstract class TimeStamp

case class Order(orderId: String, product: String, timestamp: Long) extends

case class Payment(orderId: String, payType: String, timestamp: Long) extends

object SimpleTimeIntervalJoin {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val orderData = Seq(Order("001", "iphone", 1545800002000L),
            Order("002", "mac", 1545800003000L),
            Order("003", "book", 1545800004000L),
            Order("004", "cup", 1545800018000L))

        val paymentData = Seq(Payment("001", "alipay", 1545803501000L),
            Payment("002", "card", 1545803602000L),
            Payment("003", "card", 1545803610000L),
            Payment("004", "alipay", 1545803611000L))

        val orderTable = env.fromCollection(orderData)
          .assignTimestampsAndWatermarks(new EventTimeExtractor[Order])
          .toTable(tEnv)

        val paymentTable = env.fromCollection(paymentData)
          .assignTimestampsAndWatermarks(new EventTimeExtractor[Payment])
          .toTable(tEnv)

        tEnv.registerTable("order", orderTable)
        tEnv.registerTable("payment", paymentTable)

        val sql =
            """
              |SELECT
              |
              |FROM order o
              |JOIN payment p ON o.orderId = p.orderId
              |WHERE p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' HOUR
            """.stripMargin
        tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(sql))

        val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
        result.print()
        env.execute("Interval Join")
    }
}

class EventTimeExtractor[TimeStamp]
  extends BoundedOutOfOrdernessTimestampExtractor[TimeStamp](Time.seconds(10)){
    override def extractTimestamp(t: TimeStamp): Long = {
        if (t.isInstanceOf[Order]){
            t.asInstanceOf[Order].timestamp
        } else {
            t.asInstanceOf[Payment].timestamp
        }
    }
}