package edu.wzm.retract

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

case class Order(productId: Long, amount: Int)

/**
  * Append Table
  * Retract Table
  */
object RetractTable {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)
        env.setParallelism(1)

        val dataRetract: DataStream[Order] = env.fromCollection(Seq(
            Order(1L, 4),
            Order(2L, 1),
            Order(1L, 1),
            Order(2L, 1),
            Order(3L, 2)))
        tEnv.registerDataStream("orderRetract", dataRetract, 'productId, 'amount)

        val retractResult = tEnv.sqlQuery(
            """
              |SELECT productId, sum(amount)
              |FROM orderRetract
              |GROUP BY productId
            """.stripMargin)
          .toRetractStream[Order]
        retractResult.print()

        val dataAppend: DataStream[Order] = env.fromCollection(Seq(
            Order(1L, 4),
            Order(2L, 1),
            Order(4L, 1),
            Order(3L, 2)))
        val orderAppend = tEnv.fromDataStream(dataAppend, 'productId, 'amount)

        val appendResult = tEnv.sqlQuery(
            s"""
              |SELECT productId, amount
              |FROM $orderAppend
            """.stripMargin)
          .toAppendStream[Order]
        appendResult.print()

        env.execute("Retract Table")
    }
}
