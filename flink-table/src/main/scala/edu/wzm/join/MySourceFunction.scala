package edu.wzm.join

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

class MySourceFunction[T](dataWithTimestamps: Seq[Either[(Long, T), Long]]) extends SourceFunction[T]{
    override def run(sourceContext: SourceFunction.SourceContext[T]): Unit =
        dataWithTimestamps.foreach {
            case Left(l) => sourceContext.collectWithTimestamp(l._2, l._1)
            case Right(r) => sourceContext.emitWatermark(new Watermark(r))
        }

    override def cancel(): Unit = ???
}
