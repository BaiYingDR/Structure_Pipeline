package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.exception.DataNotMatchException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}


object DataFrameUtils extends Logging {

  def asserEquals(
                   actualDF: DataFrame,
                   expectedDF: DataFrame,
                   checkColType: Boolean = false): Unit = {

    if (expectedDF.schema.fields.length != actualDF.schema.fields.length) {
      throw new DataNotMatchException(s"column length is actual and expect result are not identical")
    }
    if (checkColType) checkDataType(actualDF, expectedDF)
    val expectedDiff: Dataset[Row] = expectedDF.except(actualDF)
    val actualDiff: Dataset[Row] = actualDF.except(expectedDF)

    if (expectedDiff.count() > 0) {
      throw new DataNotMatchException(
        s"data in expected data result not present in actual data result,following is the different: \n${convertDFtoString(expectedDiff)}"
      )
    }

    if (actualDiff.count() > 0) {
      throw new DataNotMatchException(
        s"data in actual data result not present in expected data result,following is the different: \n${convertDFtoString(actualDiff)}"
      )
    }

    logInfo("actual and expectedDF data matches. ")

  }

  def convertDFtoString(df: Dataset[Row]): String =
    s"${df.schema.fields.map(_.name).mkString(",")}\n${df.collect().map(_.toString).mkString("\n")}"

  def checkDataType(actualDF: DataFrame, expectedDF: DataFrame) =
    if (!actualDF.schema.fields.zip(expectedDF.schema.fields).forall {
      case (actualField, expectedField) => actualField.dataType == expectedField.dataType
    })
      throw new DataNotMatchException(s"column type mismatch in actual and expect result")

  def optimizeHiveFileCount(dataFrame: DataFrame): Int = {
    logInfo("calculating Number of Partitions")
    val sparkSession: SparkSession = dataFrame.sparkSession
    val size: BigInt = sparkSession.sessionState.executePlan(dataFrame.queryExecution.logical).optimizedPlan.stats.sizeInBytes
    logInfo("ByteSize:" + size)
    val sizeInMB: Double = size.toLong./(1024.0)./(1024.0)
    var numPartition: Int = sizeInMB./(1024.0).ceil.toInt
    if (numPartition <= 0) {
      numPartition = 1
    } else if (numPartition > 100) {
      numPartition = 100
    }
    numPartition

  }

  def optimizeRepartition(
                           dataFrame: DataFrame,
                           partitionCount: Int,
                           partitionColumn: Option[String]): Unit = {
    val curPartitions: Int = dataFrame.rdd.getNumPartitions

    partitionColumn match {
      case Some(partitionColumn) =>
        logInfo(s"adjust partition number tyo $partitionCount from $curPartitions")
        val partitionColumnNames: Array[Column] = partitionColumn.split(",").map(elem => col(elem.mkString))
        dataFrame.repartition(partitionCount, partitionColumnNames: _*)
      case None =>
        if (partitionCount <= curPartitions) {
          logInfo(s"Decrease partition number to $partitionCount from $curPartitions")
          dataFrame.coalesce(partitionCount)
        } else {
          logInfo(s"Increase partition number to $partitionCount from $curPartitions")
          dataFrame.repartition(partitionCount)
        }
    }

  }

}
