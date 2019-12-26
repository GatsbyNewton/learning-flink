package edu.wzm.pvuv

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import java.util.{HashSet => JHashSet, Set => JSet}

/**
  * @author: Jimmy Wong
  * @Date: 2019-12-26
  * @version: 1.0
  * @Description: Example of PV and UV
  *               Test Data:
  *               111 one  1577259310000
  *               111 two  1577259313000
  *               111 one  1577259312000
  *               222 two  1577259314000
  *               one 111 1577259320000
  *               two 222 1577259323000
  *               one 111 1577259322000
  *               two 111 1577259323000
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
          .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
          .trigger(CountTrigger.of(1))
          .aggregate(new PvUvAggregator)
          .map(acc => PvUv(acc.date, acc.pv, acc.uv))
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
