package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.config.SpContext
import com.payapl.csdmp.sp.core.Writer
import com.payapl.csdmp.sp.core.output.bigquery.{BigQueryMergeWriter, BigQueryWriter}
import com.payapl.csdmp.sp.logging.{EventTracker, SpEvent}
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object OutputOperatorProcessor extends OperatorProcessorTrait with Logging {

  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo("Building output step: " + step.name)
    val writer: Writer = getWriter(step.outPutType.get.asInstanceOf[Map[String, Any]], step.name)
    logInfo("step.outputType.get.getInputDataFrameName: " + writer.dataFrameName)
    val dataFrame: DataFrame = sparkSession.table(writer.dataFrameName)
    dataFrame.printSchema()
    writer.write(dataFrame)
    loggingEvent(writer, dataFrame)
  }

  private def loggingEvent(writer: Writer, dataFrame: DataFrame)(implicit sparkSession: SparkSession): Unit = {
    if (EventTracker.shouldTracking) {
      if (writer.isInstanceOf[BigQueryWriter] || writer.isInstanceOf[BigQueryMergeWriter]) {
        val operatorType: String = writer.getClass.getSimpleName
        val (projectName, dataSetName, tableName) = if (writer.isInstanceOf[BigQueryWriter]) {
          val w: BigQueryWriter = writer.asInstanceOf[BigQueryWriter]
          (w.projectName, w.dataFrameName, w.tableName)
        } else {
          val w: BigQueryMergeWriter = writer.asInstanceOf[BigQueryMergeWriter]
          (w.projectName, w.dataFrameName, w.tableName)
        }

        val totalRow: Long = dataFrame.count()
        val batchId: String = SpContext.getBatchId
        val bucketName: String = sparkSession.conf.getOption("@bucketName").orNull
        val appName: String = SpContext.getAppName
        val timeCreated: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val data = Map(
          "operator_type" -> operatorType,
          "time_created" -> timeCreated,
          "bucket_name" -> bucketName,
          "app_name" -> appName,
          "batch_id" -> batchId,
          "total_rows" -> totalRow,
          "ext" ->
            Map(
              "project_name" -> projectName,
              "dataset_name" -> dataSetName,
              "table_name" -> tableName
            )
        )
        EventTracker.logging(SpEvent("monitoring", "operator_status_collection", data))
      }
    }
  }

  private def getWriter(outputTyoe: Map[String, Any], stepName: String = "temp"): Writer = {
    outputTyoe.map { case (name, params) => SpOperatorFactory.getOutput(name, buildParams(stepName, params)) }.headOption
      .getOrElse(throw new RuntimeException("writer is not existed..."))
  }
}
