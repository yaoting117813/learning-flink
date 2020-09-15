package com.yaoting117.learning.flink.ch01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * Flink 流处理 WordCount 程序
 */
object WordCountStreaming {

    def main(args: Array[String]): Unit = {

        val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
        val host: String = parameterTool.get("host")
        val port: Integer = parameterTool.getInt("port")

        val inputDataStream: DataStream[String] = environment.socketTextStream("dev.yaoting117.com", 7777)

        val resultDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

        resultDataStream.print().setParallelism(1)

        environment.execute()

    }

}
