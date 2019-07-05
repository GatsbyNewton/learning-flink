package edu.wzm

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
 *  Stream world!
 */
case class Order(user: Long, product: String, amount: Int)

object StreamTable{
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)

        val orderA = env.fromCollection(Seq(
            Order(1L, "beer", 3),
            Order(1L, "diaper", 4),
            Order(3L, "rubber", 2))).toTable(tEnv)

        val orderB = env.fromCollection(Seq(
            Order(2L, "pen", 3),
            Order(2L, "rubber", 3),
            Order(4L, "beer", 1))).toTable(tEnv)

        val result = orderA.unionAll(orderB)
          .select('user, 'product, 'amount)
          .where('amount > 2)
          .toAppendStream[Order]

        result.print()

        env.execute()
    }
}
