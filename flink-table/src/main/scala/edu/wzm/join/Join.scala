package edu.wzm.join

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

case class User(id: String, name: String, ts: Long)

case class Order(orderId: String, userId: String, timestamp: Long)

case class Detail(orderId: String, userId: String, username: String, timestamp: String)

/**
  * @author: Jimmy Wong
  * @Date: 2020/1/12
  * @version: 1.0
  * @Description: Dynamic Table join Dimension Table
  *
  * <p> Example of stream table join MySQL static table.
  */
object Join {
    val LOGGER = LogManager.getLogger(Join.getClass)

    val SPACE_SEPARATOR = "\\s+"

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val port = if (params.has("port")) {
            params.getInt("port")
        } else {
            System.err.println("No port specified. Please run 'Join --port <port>'")
            return
        }

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val tEnv = StreamTableEnvironment.create(env, settings)

        val userDS = env.socketTextStream("localhost", port)
            .assignAscendingTimestamps(str => str.trim.split(SPACE_SEPARATOR)(2).toLong)
            .map(str => {
                val msg = str.trim.split(SPACE_SEPARATOR)
                User(msg(0), msg(1), msg(2).toLong)
            })

        //        val rightDS = env.socketTextStream("localhost", port2)
        //          .map(str => {
        //              val msg = str.trim.split(SPACE_SEPARATOR)
        //              Order(msg(0), msg(1), msg(2).toLong)
        //          })

        tEnv.registerDataStream("user", userDS, 'id, 'name, 'ts)

        /** Event-time: specify User's ts as rowtime of Group Window */
        val user = userDS.toTable(tEnv, 'id, 'name, 'ts.rowtime)
        val result = user.window(Tumble over 10.seconds on 'ts as 'win)
            .groupBy('win, 'id)
            .select('id, 'id.count)

        result.toAppendStream[Row].print()
//        user.window(Over partitionBy 'w orderBy 'a preceding UNBOUNDED_RANGE as 'b)

        env.execute("Group/Over Windows")
    }
}
