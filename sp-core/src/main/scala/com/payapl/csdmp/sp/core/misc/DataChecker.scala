package com.payapl.csdmp.sp.core.misc

import com.payapl.csdmp.sp.core.{Misc, Reader}
import com.payapl.csdmp.sp.factory.InOutputFactory
import com.paypal.csdmp.sp.consts.Constant
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import com.paypal.csdmp.sp.utils.{GcsUtils, HdfsUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}
import java.time.Duration
import scala.collection.mutable


case class DataChecker(name: String,
                       operation: String,
                       checkInterval: Option[String],
                       maxWaitDuration: Option[String],
                       batchHour: Option[String],
                       hourCondition: Option[Int],
                       checkFilePath: Option[String],
                       isException: Option[Boolean],
                       dataSources: Option[Map[String, Any]]
                      ) extends Misc {

  override def process(keys: List[String])(implicit sparkSession: SparkSession): Unit = {
    require(checkInterval.isDefined && maxWaitDuration.isDefined, "checkInterval,maxWaitDuration is mandatory for operation: wait_for_data_availability and wait_for_file_availability")
    val intervalInMilliSeconds: Long = Duration.parse(checkInterval.get).toMillis
    var maxWaitInMilliSeconds: Long = Duration.parse(maxWaitDuration.get).toMillis
    var targetAvailable: Boolean = false

    while (maxWaitInMilliSeconds > 0) {
      operation match {
        case "wait_for_hour_availability" =>
          if (hourCondition.isEmpty) {
            throw new InvalidArgumentException("hourCondition must be set!")
          }

          if (batchHour.isEmpty) {
            throw new InvalidArgumentException("batchHour must be set!")
          }

          val hourChecker: Int = hourCondition.get
          if (0 to 23 contains hourChecker) {
            throw new InvalidArgumentException("hourCondition is not valid,please use a int number between 0 to 23!")
          }

          val actual_batch_hour: Int = if (batchHour.get.startsWith("0")) batchHour.get.substring(1, 2).toInt else batchHour.get.toInt
          targetAvailable = actual_batch_hour.equals(hourChecker)

        case "wait_for_file_availability" =>
          require(checkFilePath.isDefined, "checkFilePath is mandatory for operation: wait_for_file_availability")
          val filePath: String = checkFilePath.get

          if (filePath.startsWith(Constant.GCS_PATH_PREFIX)) {
            targetAvailable = GcsUtils.exists(filePath)
          } else if (filePath.startsWith(Constant.HDFS_PATH_PREFIX)) {
            targetAvailable = HdfsUtils.exists(filePath)
          } else {
            targetAvailable = Files.exists(Paths.get(filePath))
          }
        case "wait_for_data_availability" =>
          require(dataSources.isDefined, "dataSources is mandatory for operation: wait_for_file_availability")
          val reader: Reader = InOutputFactory.getReader(dataSources.get, name)
          val df: DataFrame = reader.read()
          targetAvailable = df.collect()(0)(0).toString.toBoolean
      }

      if (targetAvailable) {
        logInfo(s"Data Check Complete")
        return
      } else {
        logInfo(s"Data is not yet available Sleeping for $intervalInMilliSeconds before trying again")
        Thread.sleep(intervalInMilliSeconds)
        maxWaitInMilliSeconds = maxWaitInMilliSeconds - intervalInMilliSeconds
      }
    }

    if (maxWaitInMilliSeconds <= 0) {
      if (isException.getOrElse(true)) {
        val map = new mutable.HashMap[String, String]()
        map.put("@name", name)
        throw new RuntimeException("Data Checker: @name failed")
      } else {
        log.warn("Finish the job now due to data checker always fails until match the maxWaitInMilliSeconds")
        sparkSession.close()
        System.exit(0)
      }
    }

  }
}
