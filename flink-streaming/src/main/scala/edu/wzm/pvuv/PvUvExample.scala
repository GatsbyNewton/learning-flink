package edu.wzm.pvuv

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

import java.util.{Set => JSet, HashSet => JHashSet}

/**
  * @author: Jimmy Wong
  * @Date: 2019-12-26
  * @version: 1.0
  * @Description: Example of PV and UV
  */
case class Event(date: String,
                 eventId: String,
                 timestamp: Long)

case class PvUvAccumulator(var date: String,
                           var pv: Long,
                           var uv: Long,
                           /**
                             * 这里需要用Java Collection，否则报错：
                             * org.apache.flink.shaded.guava18.com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NumberFormatException: Not a version: 9
                             */
                           var ids: JSet[String])

case class PvUv(date: String,
                pv: Long,
                uv: Long)

object Driver {

    val SPACE_SEPARATOR = "\\s+"

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val stream = env.socketTextStream("localhost", 9999)

        stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
              override def extractTimestamp(msg: String): Long = {
                  msg.split(SPACE_SEPARATOR)(2).toLong
              }
          })
          .map(msg => {
              val info = msg.split(SPACE_SEPARATOR)
              Event(info(0), info(1), info(2).toLong)
          })
          .keyBy("date")
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .trigger(CountTrigger.of(1))
          .aggregate(new PvUvAggregator)
          .print()

        env.execute("PVUV")
    }
}

class PvUvAggregator extends AggregateFunction[Event, PvUvAccumulator, PvUv]{
    override def createAccumulator(): PvUvAccumulator = {
        PvUvAccumulator("", 0L, 0L, new JHashSet[String]())
    }

    override def add(value: Event, acc: PvUvAccumulator): PvUvAccumulator = {
        if (!acc.ids.contains(value.eventId)) {
            acc.ids.add(value.eventId)
            acc.uv = acc.uv + 1
            acc.date = value.date
        }
        acc.pv = acc.pv + 1

        acc
    }

    override def getResult(acc: PvUvAccumulator): PvUv = {
        PvUv(acc.date, acc.pv, acc.uv)
    }

    override def merge(acc1: PvUvAccumulator, acc2: PvUvAccumulator): PvUvAccumulator = {
        acc1.ids.addAll(acc2.ids)
        PvUvAccumulator(acc1.date, acc1.pv + acc2.pv, acc1.uv + acc2.uv, acc1.ids)
    }
}


class RandomFunction extends SourceFunction[(Long, Long)] {
    var flag = true

    override def cancel(): Unit = {
        flag = false
    }

    override def run(ctx: SourceFunction.SourceContext[(Long, Long)]): Unit = {
        while (flag) {
            val event = ((Math.random() * 1000).toLong, System.currentTimeMillis())
            ctx.collect(event)
            Thread.sleep(500)
        }
    }
}
