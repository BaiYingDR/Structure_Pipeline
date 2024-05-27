package com.payapl.csdmp.sp.core.misc.batchAuditor

import com.payapl.csdmp.sp.core.Misc
import com.payapl.csdmp.sp.factory.InOutputFactory
import com.paypal.csdmp.sp.utils.DateTimeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

case class BatchAuditor(name: String,
                        errMsg: Option[String],
                        inputDataFrame: Option[String],
                        operation: String,
                        tableName: Option[String],
                        featureSet: Option[String],
                        startDate: Option[String],
                        endDate: Option[String],
                        outputKeys: Option[List[String]],
                        sourceType: Option[Map[String, Any]],
                        sinkId: Option[String],
                        sinkType: Option[Map[String, Any]]
                       ) extends Misc {

  override def process(keys: List[String])(implicit sparkSession: SparkSession): Unit = {
    operation match {
      case "get_last_execution" =>
        require(outputKeys.isDefined && outputKeys.get.size == 1, "outputKeys is not defined properly for BatchAuditor: " + name + " for operation type: get_last_execution")
        sparkSession.conf.set("batch_start_time", DateTimeUtils.getCurrentTimestampStr)
        var startTime = ""

        // if startDate is provided & end date is provided
        if (startDate.isDefined && !startDate.get.equals("@start_timestamp")) {
          startTime = startDate.get.trim
          logInfo("Start Ts is provided")
        } else {
          logInfo("Start Ts is NOT provided.")
          val reader = InOutputFactory.getReader(sourceType.get, name)
          val df = reader.read()
          val cnt = df.count()
          if (cnt == 0 || df.rdd.collect()(0)(0) == null) {
            throw new RuntimeException("There is no records to process for this batch")
          }
          val lastProcessTs: String = df.rdd.collect()(0)(0).toString
          startTime = DateTimeUtils.formatStrTimestamp(lastProcessTs)
          logInfo("last Processed Timestamp: ")
        }
        logInfo(s"Calculated startTime: '$startTime'")
        sparkSession.conf.set(outputKeys.get(0), "'" + startTime + "'")
        sparkSession.conf.set("ctx_lowerBound", DateTimeUtils.getEpochTime(Timestamp.valueOf(startTime).toLocalDateTime))
        logInfo(s"Epoch startTime: ${DateTimeUtils.getEpochTime(Timestamp.valueOf(startTime).toLocalDateTime)}")
      case "get_max_execution" =>
        require(outputKeys.isDefined && outputKeys.get.size == 1, "outputKeys is not defined properly for BatchAuditor: " + name + " for operation type: get_max_execution")
        var endTime = ""

        // if startDate is provided & end date is provided
        if (endDate.isDefined && !endDate.get.equals("@end_timestamp")) {
          endTime = endDate.get.trim
          logInfo("End Ts is provided")
        } else {
          logInfo("End Ts is NOT provided.")
          val reader = InOutputFactory.getReader(sourceType.get, name)
          val df = reader.read()
          val cnt = df.count()
          if (cnt == 0 || df.rdd.collect()(0)(0) == null) {
            throw new RuntimeException("There is no records to process for this batch")
          }
          val maxProcessTs: String = df.rdd.collect()(0)(0).toString
          endTime = DateTimeUtils.formatStrTimestamp(maxProcessTs)
          logInfo("last Processed Timestamp: ")
        }
        logInfo(s"Calculated endTime: '$endTime'")
        sparkSession.conf.set(outputKeys.get(0), "'" + endTime + "'")
        sparkSession.conf.set("max_process_ts", endTime)
        sparkSession.conf.set("ctx_lowerBound", DateTimeUtils.getEpochTime(Timestamp.valueOf(endTime).toLocalDateTime))
        logInfo(s"Epoch startTime: ${DateTimeUtils.getEpochTime(Timestamp.valueOf(endTime).toLocalDateTime)}")
      case "update_max_processed_ts" =>
        require(outputKeys.isDefined && outputKeys.get.size == 1, "outputKeys is not defined properly for BatchAuditor: " + name + " for operation type: update_max_processed_ts")
        val tableExists: Boolean = sparkSession.catalog.tableExists(inputDataFrame.get)
        var noOfRecordProcessed: Long = 0
        if (tableExists) {
          val df: DataFrame = sparkSession.sql("select * from " + inputDataFrame.get)
          noOfRecordProcessed = df.count()
        }

        val max_processed_ts = sparkSession.conf.get("max_processed_ts", DateTimeUtils.getCurrentTimestampStr)
        logInfo(s"max_processed_ts: $max_processed_ts")
        val max_ts: Timestamp = Timestamp.valueOf(max_processed_ts)

        val batchStartTime = Timestamp.valueOf(sparkSession.conf.get("batch_start_time", DateTimeUtils.getCurrentTimestampStr))
        val batchEndTime = Timestamp.valueOf(DateTimeUtils.getCurrentTimestampStr)

        var errMessage = ""
        if (errMsg.isDefined && errMsg.get.nonEmpty) {
          errMessage = errMsg.get
        } else {
          try {
            errMessage = sparkSession.conf.get("app_error_message")
          } catch {
            case _: Exception =>
              errMessage = ""
          }
        }

        var status = "Success"
        if (errMessage.nonEmpty) {
          status = "Failed"
        }

        var batchType = "FEATURE_JOB"
        var endTime = ""
        // if startDate is provided & end date is provided
        if (endDate.isDefined && !endDate.get.equals("@end_timestamp")) {
          endTime = endDate.get.trim
          logInfo
          ("End Ts is provided")
        } else {
          logInfo("End Ts is NOT provided.")
          val reader = InOutputFactory.getReader(sourceType.get, name)
          val df = reader.read()
          val cnt = df.count()
          if (cnt == 0 || df.rdd.collect()(0)(0) == null) {
            throw new RuntimeException("There is no records to process for this batch")
          }
          val maxProcessTs: String = df.rdd.collect()(0)(0).toString
          endTime = DateTimeUtils.formatStrTimestamp(maxProcessTs)
          logInfo("last Processed Timestamp: ")
        }
        logInfo(s"Calculated endTime: '$endTime'")
        sparkSession.conf.set(outputKeys.get(0), "'" + endTime + "'")
        sparkSession.conf.set("max_process_ts", endTime)
        sparkSession.conf.set("ctx_lowerBound", DateTimeUtils.getEpochTime(Timestamp.valueOf(endTime).toLocalDateTime))
        logInfo(s"Epoch startTime: ${DateTimeUtils.getEpochTime(Timestamp.valueOf(endTime).toLocalDateTime)}")
      case "audit_log" =>
    }
  }


  def getAuditLof(auditTableId: String): String = {
    null
  }

  def writeHiveAuditSink(auditTableId: String)(implicit sparkSession: SparkSession): DataFrame = {
    null
  }

  def getAuditDf()(implicit sparkSession: SparkSession): DataFrame = {
    null
  }

  def writeBQAuditLog(auditTableId: String)(implicit sparkSession: SparkSession): Unit = {

  }

}
