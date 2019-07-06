package edu.wzm.source

import java.io.File

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.types.Row

object Main {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val sourceTableName = "source"
        val tableSource = new MyTableSource

        val sinkTableName = "csvSink"
        val tableSink = getCsvTableSink

        tEnv.registerTableSource(sourceTableName, tableSource)
        tEnv.registerTableSink(sinkTableName, tableSink)

        val result = tEnv.scan(sourceTableName)
          .window(Tumble over 2.minute on 'accessTime as 'w)
          .groupBy('w, 'region)
          .select('region, 'w.start, 'w.end, 'region.count as 'pv)

        result.insertInto(sinkTableName)
        env.execute()
    }

    def getCsvTableSink: TableSink[Row] = {
        val tmpFile = File.createTempFile("csv_sink_", "tmp")
        println(s"Sink path: $tmpFile" )

        if (tmpFile.exists()){
            tmpFile.delete()
        }

        new CsvTableSink(tmpFile.getAbsolutePath).configure(
            Array[String]("region", "winStart", "winEnd", "pv"),
            Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG)
        )
    }
}
