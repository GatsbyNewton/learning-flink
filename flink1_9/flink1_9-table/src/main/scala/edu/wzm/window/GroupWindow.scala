package edu.wzm.window

import java.util.{Objects, Properties}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

/**
 * @author: Jimmy Wong
 * @Date: 2020/1/21
 * @version: 1.0
 * @Description: Example of Group Window(Processing-Time and Event-Time)
 *               <p> Test Data:
 *               {"id":10001,"name":"bill","opTime":"2019-12-25T15:35:11Z"}
 *               {"id":10002,"name":"bill","opTime":"2019-12-25T15:35:12Z"}
 *               {"id":10001,"name":"bill","opTime":"2019-12-25T15:35:13Z"}
 *               {"id":10001,"name":"bill","opTime":"2019-12-25T15:35:21Z"}
 */
object GroupWindow {
    val LOGGER = LogManager.getLogger(GroupWindow.getClass)

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val topic = if (params.has("topic")) {
            params.get("topic")
        } else {
            System.err.println("No port specified. Please run 'GroupWindow --port <port> --timeType <proc|event>'")
            return
        }
        val timeType = if (params.has("timeType")) {
            params.get("timeType")
        } else {
            System.err.println("No port specified. Please run 'GroupWindow --port <port> --timeType <proc|event>'")
            return
        }

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        if (Objects.equals(timeType, "proc")) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        } else {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        }

        val settings = EnvironmentSettings.newInstance()
          .inStreamingMode()
          .useBlinkPlanner()
          .build()
        val tEnv = StreamTableEnvironment.create(env, settings)

        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "localhost:9092")
        prop.setProperty("group.id", "test")
        val kafka: Kafka = new Kafka()
          .properties(prop)
          .topic(topic)
          .version("0.10")

        if (Objects.equals(timeType, "proc")) {
            computeByProcessingTime(kafka, env, tEnv)
        } else {
            computeByEventTime(kafka, env, tEnv)
        }

        env.execute("Group Window")
    }

    def computeByProcessingTime(kafka: Kafka, env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment): Unit = {
        val schema: Schema = new Schema()
          .field("id", Types.STRING)
          .field("name", BasicTypeInfo.STRING_TYPE_INFO)
          .field("opTime", Types.LONG)
          /** Processing-Time: specify proctime, and must be the latest field */
          .field("procTime", Types.SQL_TIMESTAMP).proctime()

        tEnv.connect(kafka)
          .withSchema(schema)
          .withFormat(new Json().deriveSchema())
          .inAppendMode()
          .registerTableSource("Users")

        val table = tEnv.sqlQuery("SELECT * FROM Users")
        table.toAppendStream[Row].print("row")

        val tumbleSql = tEnv.sqlQuery(
            """
              |SELECT
              | id,
              | TUMBLE_START(procTime, INTERVAL '10' SECOND) AS wStart,
              | TUMBLE_END(procTime, INTERVAL '10' SECOND) AS wEnd,
              | TUMBLE_PROCTIME(procTime, INTERVAL '10' SECOND) AS pTime,
              | COUNT(id)
              |FROM Users
              |GROUP BY TUMBLE(procTime, INTERVAL '10' SECOND), id
              |""".stripMargin)
        tumbleSql.toAppendStream[Row].print("tumble sql")
    }

    def computeByEventTime(kafka: Kafka, env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment): Unit = {
        val schema: Schema = new Schema()
          .field("id", Types.STRING)
          .field("name", BasicTypeInfo.STRING_TYPE_INFO)
          /** Event-Time: specify opTime as rowtime */
          .field("rowTime", Types.SQL_TIMESTAMP)
          .rowtime(new Rowtime().timestampsFromField("opTime").watermarksPeriodicBounded(0))

        tEnv.connect(kafka)
          .withSchema(schema)
          .withFormat(new Json().deriveSchema())
          .inAppendMode()
          .registerTableSource("Users")

        val table = tEnv.sqlQuery("SELECT * FROM Users")
        table.toAppendStream[Row].print("row")

        val tumbleTab = table.window(Tumble over 10.seconds on 'rowTime as 'w)
          .groupBy('w, 'id)
          .select('id, 'w.start as 'start, 'w.end as 'end, 'id.count)
        tumbleTab.toAppendStream[Row].print("tumble tab")

        val tumbleSql = tEnv.sqlQuery(
            """
              |SELECT
              | id,
              | TUMBLE_START(rowTime, INTERVAL '10' SECOND) AS wStart,
              | TUMBLE_END(rowTime, INTERVAL '10' SECOND) AS wEnd,
              | TUMBLE_ROWTIME(rowTime, INTERVAL '10' SECOND) AS opTime,
              | COUNT(id)
              |FROM Users
              |GROUP BY TUMBLE(rowTime, INTERVAL '10' SECOND), id
              |""".stripMargin)
        tumbleSql.toAppendStream[Row].print("tumble sql")

        val slideSql = tEnv.sqlQuery(
            """
              |SELECT
              | id,
              | HOP_START(rowTime, INTERVAL '10' SECOND, INTERVAL '5' SECOND) AS wStart,
              | HOP_END(rowTime, INTERVAL '10' SECOND, INTERVAL '5' SECOND) AS wEnd,
              | HOP_ROWTIME(rowTime, INTERVAL '10' SECOND, INTERVAL '5' SECOND) AS opTime,
              | COUNT(id)
              |FROM Users
              |GROUP BY HOP(rowTime, INTERVAL '10' SECOND, INTERVAL '5' SECOND), id
              |""".stripMargin)
        slideSql.toAppendStream[Row].print("sliding sql")
    }
}
