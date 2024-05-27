package com.payapl.csdmp.sp.listener

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerEnvironmentUpdate, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SparkMetricsListener {
  private var maxPeakMemory = 0L
  private var totalSpilledDisk = 0L
  private var totalStorageUsage = 0L
  private var clusterName: String = ""
  private var applicationId: String = ""
  private var applicationName: String = ""
  private var scalaVersion = "2.12"
  private var sparkVersion = "3.13"

  def getMetricsInfo(spark: SparkSession): Unit = {
    val info = s"maxPeakMemory = ${SparkMetricsListener.maxPeakMemory / (1024 * 1024.0)} MB and DiskUsage - " +
      s"${SparkMetricsListener.totalSpilledDisk / (1024 * 1024.0)} MB and StorageUsage - " +
      s"${SparkMetricsListener.totalStorageUsage / (1024 * 1024.0)} MB"

    spark.sparkContext.setJobGroup("SP-Metric", info)
    spark.sparkContext.parallelize(Seq(info)).count()
  }

  def getAuditlogData: mutable.Map[String, String] = {
    mutable.Map(
      "MaxPeakMemory" -> s"${SparkMetricsListener.maxPeakMemory / (1024 * 1024.0)} MB",
      "DiskUsage" -> s"${SparkMetricsListener.totalSpilledDisk / (1024 * 1024.0)} MB",
      "StorageUsage" -> s"${SparkMetricsListener.totalStorageUsage / (1024 * 1024.0)} MB"
    )
  }

  private def setApplicationName(name: String): Unit = {
    if (applicationName.isEmpty)
      applicationName = name
  }

  private def setApplicationId(id: String): Unit = {
    if (applicationId.isEmpty)
      applicationId = id
  }

  private def setClusterName(name: String): Unit = {
    if (clusterName.isEmpty)
      clusterName = name
  }
}

class SparkMetricsListener extends SparkListener with Logging {
  private val taskPeakMemoryMap: mutable.Map[Long, Long] = mutable.Map[Long, Long]()
  private val taskSpilledDiskMap: mutable.Map[Long, Long] = mutable.Map[Long, Long]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskMetrics != null) {
      val stageId: Int = taskEnd.stageId
      val diskSpilled: Long = taskEnd.taskMetrics.diskBytesSpilled
      val peakMemory: Long = taskEnd.taskMetrics.peakExecutionMemory
      taskPeakMemoryMap.update(stageId, taskPeakMemoryMap.getOrElse(stageId, 0L) + peakMemory)
      taskSpilledDiskMap.update(stageId, taskSpilledDiskMap.getOrElse(stageId, 0L) + diskSpilled)
    } else {
      logWarning(s"Failed to analyze task metric due to task ${taskEnd.taskInfo.taskId} failed when onTaskEnd")
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    try {
      val stageId: Int = stageCompleted.stageInfo.stageId
      SparkMetricsListener.maxPeakMemory = Math.max(SparkMetricsListener.maxPeakMemory, taskPeakMemoryMap.getOrElse(stageId, 0L))
      SparkMetricsListener.totalSpilledDisk = Math.max(SparkMetricsListener.totalSpilledDisk, taskPeakMemoryMap.getOrElse(stageId, 0L))
      SparkMetricsListener.totalStorageUsage += stageCompleted.stageInfo.rddInfos.filter(_.storageLevel.useMemory).map(_.memSize).sum
    } catch {
      case _: Exception =>
        logWarning("Fail to analyze metric when onStageCompleted")
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (jobStart.properties != null) {
      SparkMetricsListener.setApplicationId(jobStart.properties.getProperty("spark.app.id", ""))
      SparkMetricsListener.setApplicationName(jobStart.properties.getProperty("spark.app.name", ""))
      var host: String = jobStart.properties.getProperty("spark.driver.host", "")
      if (host.endsWith("-m")) host = host.dropRight(2)
      SparkMetricsListener.setClusterName(host)
    }

  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    SparkMetricsListener.setApplicationId(applicationStart.appId.getOrElse(""))
    SparkMetricsListener.setApplicationId(applicationStart.appName)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    try {
      logInfo("Involve in onEnvironmentUpdate event")
      val sparkCorePath: String = environmentUpdate.environmentDetails.getOrElse("classpath Entries", Seq()).find(item => item._1.contains("spark-core")).getOrElse(("/usr/lib/spark/jars/spark-core_2.12-3.1.3.jar", "System Classpath"))._1
      SparkMetricsListener.scalaVersion = sparkCorePath.substring(sparkCorePath.indexOf("spark-core_") + 11).split("-")(0)
      SparkMetricsListener.sparkVersion = sparkCorePath.substring(sparkCorePath.indexOf("spark-core_") + 11).split("-")(1).dropRight(4)
    } catch {
      case _: Exception =>
        logWarning("Fail to analyze Spark core classpath and skip get scala and spark version")
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    try {
      var host: String = blockManagerAdded.blockManagerId.host
      if (blockManagerAdded.blockManagerId.executorId == "driver") {
        if (host.endsWith("-m")) host = host.dropRight(2)
        SparkMetricsListener.setClusterName(host)
      }
    } catch {
      case _: Exception => logWarning("Failed to analyze Spark Cluster when onBlockManagerAdded event")
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    // print metrics table with vertical headers
    println("+" + "-" * 26 + "+" + "-" * 32 + "+")
    println("|" + "-" * 13 + "application information summary" + "-" * 15 + "|")
    println("+" + "-" * 26 + "+" + "-" * 32 + "+")

    printf("| %-24s | %30s |\n", "Application ID", SparkMetricsListener.applicationId)
    printf("| %-24s | %30s |\n", "Application Name", SparkMetricsListener.applicationName)
    printf("| %-24s | %30s |\n", "Cluster Name", SparkMetricsListener.clusterName)
    printf("| %-24s | %30s |\n", "scala Version", SparkMetricsListener.scalaVersion)
    printf("| %-24s | %30s |\n", "spark Version", SparkMetricsListener.sparkVersion)

    println("+" + "-" * 26 + "+" + "-" * 32 + "+")
    println("|" + "-" * 13 + "metric information summary" + "-" * 20 + "|")
    println("+" + "-" * 26 + "+" + "-" * 32 + "+")

    printf("| %-24s | %27.2f MB |\n", "Total Disk Usage", SparkMetricsListener.totalSpilledDisk / (1024 * 1024.0))
    printf("| %-24s | %27.2f MB |\n", "Total Storage Usage", SparkMetricsListener.totalStorageUsage / (1024 * 1024.0))
    printf("| %-24s | %27.2f MB |\n", "Maximum Execution Memory", SparkMetricsListener.maxPeakMemory / (1024 * 1024.0))

    println("+" + "-" * 26 + "+" + "-" * 32 + "+")
  }


}
