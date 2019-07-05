package edu.wzm.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer

class MySink extends RichSinkFunction[(Boolean, Row)]{

    val results = new ArrayBuffer[String]()

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
    }

    override def invoke(value: (Boolean, Row), context: SinkFunction.Context[_]): Unit = {
        results.synchronized{
            val res = value._2.toString
        }
    }

    override def clone(): AnyRef = super.clone()
}
