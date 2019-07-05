package edu.wzm.join

import java.lang.{Long => JLong}
import java.util
import java.util.Collections

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.types.Row

class MyTableSource extends StreamTableSource[Row] with DefinedRowtimeAttributes{

    val fieldNames = Array("accessTime", "region", "userId")
    val schema = new TableSchema(fieldNames, Array(Types.SQL_TIMESTAMP, Types.STRING, Types.STRING))
    val rowType = new RowTypeInfo(
        Array(Types.LONG, Types.STRING, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        fieldNames)

    val data = Seq(
        Left(1510365660000L, Row.of(new JLong(1510365660000L), "ShangHai", "U0010")),
        Right(1510365660000L),
        Left(1510365660000L, Row.of(new JLong(1510365660000L), "BeiJing", "U1001")),
        Right(1510365660000L),
        Left(1510366200000L, Row.of(new JLong(1510366200000L), "BeiJing", "U2032")),
        Right(1510366200000L),
        Left(1510366260000L, Row.of(new JLong(1510366260000L), "BeiJing", "U1100")),
        Right(1510366260000L),
        Left(1510373400000L, Row.of(new JLong(1510373400000L), "ShangHai", "U0011")),
        Right(1510373400000L))

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
        execEnv.addSource(new MySourceFunction[Row](data))
          .returns(rowType)
          .setParallelism(1)
    }

    override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
        Collections.singletonList(new RowtimeAttributeDescriptor(
            "accessTime",
            new ExistingField("accessTime"),
            PreserveWatermarks.INSTANCE))
    }

    override def getReturnType: TypeInformation[Row] = rowType

    override def getTableSchema: TableSchema = schema
}
