package com.sql

import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, TableSchema}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.types.Row
import java.io.File
import java.util
import java.util.Collections

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks
import org.apache.flink.table.sources.{DefinedRowtimeAttributes, RowtimeAttributeDescriptor, StreamTableSource}


/**
 * @author liugou.
 * @date 2021/8/6 14:12
 */
object SimpleDemo {

  def main(args: Array[String]): Unit = {

    //streaming环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val setting = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = TableEnvironment.create(setting)
    //设置eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //方便我们查出输出数据
    env.setParallelism(1)

    val sourceTableName = "mySource"

    //创建自定义source数据结构
    val tableSource = new MyTableSource

    val sinkTableName = "csvSink"
    // 创建CSV sink 数据结构
    val tableSink = getCsvTableSink

    //注册source
    tEnv.registerTableSource(sourceTableName,tableSource)

    //注册sink
    tEnv.registerTableSink(sinkTableName,tableSink)

    val sql =
      s"""
        |select region,
        |TUMBLE_START(accessTime,INTERVAL '2' MINUTE) as winStart,
        |TUMBLE_END(accessTime,INTERVAL '2' MINUTE) as winEnd,
        |count(region) as pv from mySource
        |group by TUMBLE(accessTime,Interval '2' minute) region
        |""".stripMargin
    tEnv.sqlQuery(sql).insertInto(sinkTableName)
    env.execute()
  }



  def getCsvTableSink:TableSink[Row] = {
      val tempFile = File.createTempFile("csv_sink_","tem")
    println("Sink path : " + tempFile)
    if(tempFile.exists()) tempFile.delete()
    new CsvTableSink(tempFile.getAbsolutePath).configure(
      Array[String]("region","winStart","winEnd","pv"),
      Array[TypeInformation[_]](Types.STRING,Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP,Types.LONG)
    )
  }

}

class MyTableSource extends StreamTableSource[Row] with DefinedRowtimeAttributes {

  val fileNames = Array("accessTime","region","userId")
  val schema= new TableSchema(fileNames,Array(Types.SQL_TIMESTAMP,Types.STRING,Types.STRING))

  val rowType = new RowTypeInfo(
    Array(Types.LONG,Types.STRING,Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
    fileNames)

  //页面访问表数据 rows with timestamps and watermarks
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
    Right(1510373400000L)
  )

  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.addSource(new MySourceFunction[Row](data)).setParallelism(1).returns(rowType)
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    Collections.singletonList(new RowtimeAttributeDescriptor(
      "accessTime",
      new ExistingField("accessTime"),
      PreserveWatermarks.INSTANCE))
  }

  override def getTableSchema: TableSchema = schema

  override def getReturnType: TypeInformation[Row] = rowType
}

//water mark 生成器
class MySourceFunction[T](dataWithTimestampList:Seq[Either[(Long,T),Long]]) extends SourceFunction[T]{
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    dataWithTimestampList.foreach{
      case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
      case Right(w) => ctx.emitWatermark(new Watermark(w))
    }
  }

  override def cancel(): Unit = ???
}

