package edu.wzm.pvuv

import org.apache.flink.api.common.state._
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.DateTime

/**
  * @author jimmy
  * @date 2019/10/20
  * @description
  * @version
  */
case class Event(eventId: String, date: String, timestamp: Long)

case class EventWindow(date: String, eventId: String, windowEnd: Long)

case class PvUv(date: String, pv: Long, uv: Long)

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
                  msg.split(SPACE_SEPARATOR)(1).toLong
              }
          })
          .map(msg => {
              val info = msg.split(SPACE_SEPARATOR)
              Event(info(0), new DateTime(info(1).toLong).toString("yyyy-MM-dd"), info(1).toLong)
          })
          .keyBy("date")
          .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
          .trigger(CountTrigger.of(1))
          .apply(new PvUvWindowFunction)
          .keyBy("windowEnd")
          .process(new PvUvProcessFunction)
          .print()

        env.execute("PVUV")
    }
}

class PvUvWindowFunction extends WindowFunction[Event, EventWindow, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Event], out: Collector[EventWindow]): Unit = {
        val it = input.iterator
        var event = it.next
        while (it.hasNext) {
            event = it.next()
        }
        out.collect(EventWindow(event.date, event.eventId, window.getEnd))
    }
}

class PvUvProcessFunction extends KeyedProcessFunction[Tuple, EventWindow, PvUv] {
    var state: MapState[String, Long] = _

    override def open(parameters: Configuration): Unit = {
        state = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("uv", classOf[String], classOf[Long]))
    }

    override def processElement(input: EventWindow, context: KeyedProcessFunction[Tuple, EventWindow, PvUv]#Context, collector: Collector[PvUv]): Unit = {
        if (state.contains(input.eventId)) {
            state.put(input.eventId, state.get(input.eventId) + 1)
        } else {
            state.put(input.eventId, 1)
        }

        var uv = 0L
        var pv = 0L
        val it = state.values().iterator()
        while (it.hasNext) {
            uv += 1
            pv += it.next
        }

        collector.collect(PvUv(input.date, pv, uv))
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, EventWindow, PvUv]#OnTimerContext, out: Collector[PvUv]): Unit = {
        state.clear()
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
