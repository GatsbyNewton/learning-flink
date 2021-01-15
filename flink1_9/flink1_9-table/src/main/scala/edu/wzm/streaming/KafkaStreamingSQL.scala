package edu.wzm.streaming

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Simple example for registering Kafka Source with DDL.
  *
  * <p> Description:
  *  - data source: flink-table/data/user-behavior.log
  *  - kafka producer: flink-table/bin/streaming/kafka-producer.sh
  *  - sql: flink-table/sql/streaming_sql.sql
  */
object KafkaStreamingSQL {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()

        val tEnv = StreamTableEnvironment.create(env, settings)
        val createTable =
            """
              |CREATE TABLE user_behavior (
              |    user_id VARCHAR COMMENT '用户ID',
              |    item_id VARCHAR COMMENT '商品ID',
              |    category_id VARCHAR COMMENT '类别ID',
              |    behavior VARCHAR COMMENT '操作行为',
              |    ts TIMESTAMP
              |)
              |WITH (
              |    'connector.type' = 'kafka', -- 使用 kafka connector
              |    'connector.version' = '0.10',  -- kafka 版本
              |    'connector.topic' = 'user-behavior',  -- kafka topic
              |    'connector.startup-mode' = 'earliest-offset', -- 从起始 offset 开始读取
              |    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
              |    'connector.properties.0.value' = 'localhost:2181',
              |    'connector.properties.1.key' = 'bootstrap.servers',
              |    'connector.properties.1.value' = 'localhost:9092',
              |    'update-mode' = 'append',
              |    'format.type' = 'json',  -- 数据源格式为 json
              |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
              |)
            """.stripMargin

        tEnv.sqlUpdate(createTable)

        val query =
            """
              |SELECT
              |  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
              |  COUNT(*) AS pv,
              |  COUNT(DISTINCT user_id) AS uv
              |FROM user_behavior
              |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')
            """.stripMargin

        val result = tEnv.sqlQuery(query)
        result.toRetractStream[Row].print()

        tEnv.execute("PV UV")
    }
}

