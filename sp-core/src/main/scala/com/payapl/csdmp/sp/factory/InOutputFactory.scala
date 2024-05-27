package com.payapl.csdmp.sp.factory

import com.payapl.csdmp.sp.core.input.bigquery.BigQueryReader
import com.payapl.csdmp.sp.core.input.file.FileReader
import com.payapl.csdmp.sp.core.input.hive.HiveReader
import com.payapl.csdmp.sp.core.input.kafka.KafkaReader
import com.payapl.csdmp.sp.core.input.oracle.OracleReader
import com.payapl.csdmp.sp.core.output.ShowWriter
import com.payapl.csdmp.sp.core.output.bigquery.BigQueryWriter
import com.payapl.csdmp.sp.core.output.csv.CsvWriter
import com.payapl.csdmp.sp.core.output.file.FileWriter
import com.payapl.csdmp.sp.core.output.hdfs.HdfsWriter
import com.payapl.csdmp.sp.core.output.hive.{HiveMergeWriter, HiveWriter}
import com.payapl.csdmp.sp.core.output.kafka.KafkaWriter
import com.payapl.csdmp.sp.core.{Reader, Writer}
import com.paypal.csdmp.sp.utils.ReflectionUtils.beanFromMap

object InOutputFactory {

  private def getInput(name: String, params: Map[String, Any]): Reader = {
    name match {
      case "oracleInput" => beanFromMap[OracleReader](params)
      case "fileInput" => beanFromMap[FileReader](params)
      case "hiveInput" => beanFromMap[HiveReader](params)
      case "bigQueryInput" => beanFromMap[BigQueryReader](params)
      case "gcsInput" => beanFromMap[FileReader](params)
//      case "teradataInput" => beanFromMap[TeradataReader](params)
      case "kafkaInput" => beanFromMap[KafkaReader](params)
      case _ => throw new IllegalArgumentException(s"$name input type is not valid")
    }
  }

  private def getOutput(name: String, params: Map[String, Any]): Writer = {
    name match {
      case "hiveOutput" => beanFromMap[HiveWriter](params)
      case "gcsOutput" => beanFromMap[FileWriter](params)
      case "bigQueryOutput" => beanFromMap[BigQueryWriter](params)
      case "showOutput" => beanFromMap[ShowWriter](params)
      case "hdfsOutput" => beanFromMap[HdfsWriter](params)
      case "csvOutput" => beanFromMap[CsvWriter](params)
      case "hiveMergeOutput" => beanFromMap[HiveMergeWriter](params)
      case "bigQueryMergeOutput" => beanFromMap[BigQueryWriter](params)
      case "kafkaOutput" => beanFromMap[KafkaWriter](params)
      case "fileOutput" => beanFromMap[FileWriter](params)
      case _ => throw new IllegalArgumentException(s"$name output type is not valid")
    }
  }

  def getReader(inputType: Map[String, Any], stepName: String = "temp"): Reader = {
    inputType.map {
      case (name, params) => getInput(name, buildParams(stepName, params))
    }.headOption.getOrElse(throw new IllegalArgumentException(s"reader is not existed"))
  }

  def getWriter(outputType: Map[String, Any], stepName: String = "temp"): Writer = {
    outputType.map {
      case (name, params) => getOutput(name, buildParams(stepName, params))
    }.headOption.getOrElse(throw new IllegalArgumentException(s"writer is not existed"))
  }

  def buildParams(name: String, params: Any): Map[String, Any] = params.asInstanceOf[Map[String, Any]] + ("name" -> name)

}
