package edu.wzm.join.dimension

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

case class Order(orderId: String, userId: Int, opTime: Long)

case class Detail(orderId: String, userId: Int, username: String, opTime: Long)

/**
  * @author: Jimmy Wong
  * @Date: 2020/1/12
  * @version: 1.0
  * @Description: Dynamic Table join Dimension Table
  *
  * <p> Example of stream table join MySQL static table.
  *     MySQL table:
  *          CREATE TABLE user (
  *            user_id int(11) NOT NULL DEFAULT 0 AUTO_INCREMENT,
  *            username varchar(16) DEFAULT NULL,
  *            PRIMARY KEY (user_id)
  *          )
  */
object DimensionTableJoin {
    val LOGGER = LogManager.getLogger(DimensionTableJoin.getClass)

    val SPACE_SEPARATOR = "\\s+"

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val port = if (params.has("port")) {
            params.getInt("port")
        } else {
            System.err.println("No port specified. Please run 'DimensionTableJoin --port <port>'")
            return
        }

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
        val tEnv = StreamTableEnvironment.create(env, settings)

        val typeInformation = Array[TypeInformation[_]] (
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        )
        val rowTypeInfo = new RowTypeInfo(typeInformation,
            Array[String]("id", "username"))

        val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.cj.jdbc.Driver")
            .setDBUrl("jdbc:mysql://localhost:3306/test")
            .setUsername("root")
            .setPassword("123456")
            .setRowTypeInfo(rowTypeInfo)
            .setQuery("SELECT * FROM user")
            .finish()

        val socketDS: DataStream[Order] = env.socketTextStream("localhost", port)
            .map(str => {
                val msg = str.trim.split(SPACE_SEPARATOR)
                Order(msg(0), msg(1).toInt, msg(2).toLong)
            })

        val dimensionDS = env.createInput(jdbcInputFormat)

        tEnv.registerDataStream("streaming", socketDS, 'orderId, 'userId, 'opTime)
        tEnv.registerDataStream("dimension", dimensionDS, 'id, 'username)

        val tabResult = tEnv.sqlQuery(
            """
              |SELECT s.orderId, d.*, s.opTime
              |FROM streaming s
              |INNER JOIN dimension d ON s.userId = d.id
              |""".stripMargin)
        tabResult.toAppendStream[Row]print()

        val stTable = tEnv.sqlQuery("SELECT * FROM streaming")
        val dimTable = tEnv.sqlQuery("SELECT * FROM dimension")
        val sqlResult = stTable.join(dimTable)
            .where("userId = id")
            .select('orderId, 'userId, 'username, 'opTime)
        sqlResult.toAppendStream[Detail].print()

        env.execute("Dimension Table Join")
    }
}
