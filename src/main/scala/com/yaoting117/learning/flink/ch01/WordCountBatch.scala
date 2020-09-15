package com.yaoting117.learning.flink.ch01

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


object WordCountBatch {

    def main(args: Array[String]): Unit = {

        val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        val inputPath: String = "G:\\0916\\big-data\\learning-flink\\src\\main\\resources\\hello.txt"

        val inputDataSet: DataSet[String] = environment.readTextFile(inputPath)

        val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

        resultDataSet.print()

    }

}
