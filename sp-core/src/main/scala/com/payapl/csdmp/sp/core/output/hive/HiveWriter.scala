package com.payapl.csdmp.sp.core.output.hive

import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.consts.Constant.DATASOURCE_FORMAT_HIVE
import com.paypal.csdmp.sp.utils.DataFrameUtils
import com.paypal.csdmp.sp.utils.SparkFunctionUtils.getColType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

case class HiveWriter(name: String,
                      database: String,
                      dataFrameName: String,
                      tableName: String,
                      partitionColumn: Option[String],
                      batchId: Option[String],
                      outputMode: Option[String],
                      adjust: Option[Boolean],
                      repartition: Option[Boolean],
                      localFileListPath: Option[String],
                      hdfsFileListPath: Option[String],
                      hdfsDeleteFilePath: Option[String],
                      format: Option[String]
                     ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {

    logInfo("HiveWriter table: " + tableName)
    dataFrame.sparkSession.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    dataFrame.sparkSession.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    dataFrame.sparkSession.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

    val tableNameWithDataBase = s"$database.$tableName"
    val tableExists: Boolean = dataFrame.sparkSession.catalog.tableExists(tableNameWithDataBase)
    var df: DataFrame = dataFrame
    if (batchId.isDefined && batchId.get.nonEmpty) {
      df.withColumn("etl_date", functions.lit(batchId.get))
    }

    if (adjust.isDefined && adjust.get) {
      adjustTable(tableNameWithDataBase, df, df.sparkSession, tableExists)
    }

    if (repartition.isDefined && repartition.get) {
      df.cache.foreach(_ => ())
      val partitionCount: Int = DataFrameUtils.optimizeHiveFileCount(df)
      logInfo(s"partitionCount is $partitionCount")
      df = DataFrameUtils.optimizeRepartition(df, partitionCount, partitionColumn)
    }

    val mode: String = outputMode.getOrElse("overwrite")
    partitionColumn match {
      case Some(partitionColumn) =>
        val partitionColumnNames: Array[String] = partitionColumn.split(",").map(elem => elem.mkString)
        val partitionColumnNamesSq = Seq(partitionColumnNames: _*)
        if (tableExists) {
          df.write.mode(mode).format(format.getOrElse(DATASOURCE_FORMAT_HIVE)).insertInto(tableNameWithDataBase)
        } else {
          df.write.mode(mode).format(format.getOrElse(DATASOURCE_FORMAT_HIVE)).partitionBy(partitionColumnNamesSq: _*).saveAsTable(tableNameWithDataBase)
        }

      case None =>
        if (tableExists) {
          df.write.mode(mode).format(format.getOrElse(DATASOURCE_FORMAT_HIVE)).insertInto(tableNameWithDataBase)
        } else {
          df.write.mode(mode).format(format.getOrElse(DATASOURCE_FORMAT_HIVE)).saveAsTable(tableNameWithDataBase)
        }
    }
  }

  def adjustTable(tableName: String, dataFrame: DataFrame, spark: SparkSession, tableExists: Boolean) = {
    if (tableExists) {
      val sourceDF: DataFrame = spark.read.table("tableName")
      val sourceDFColumn: Array[String] = sourceDF.columns.map(i => i.toLowerCase)
      var deltaColumns: String = ""

      for (column <- dataFrame.columns) {
        if (!sourceDFColumn.contains(column.toLowerCase())) {
          deltaColumns = deltaColumns + "`" + column + "`" + getColType(dataFrame, column) + ","
        }
      }

      if (deltaColumns.nonEmpty) {
        val query: String = "Alter Table " + tableName + "ADD COLUMNS(" + deltaColumns.subSequence(0, deltaColumns.length - 1) + ")"
        logInfo(s"Alter Query Statement : $query")
        spark.sql(query)
      }
    }
  }

  def getOutputFileCount(dataFrame: DataFrame): Int = {

    logInfo("Calculating Number of partitions")

    val sparkSession: SparkSession = dataFrame.sparkSession
    val size: BigInt = sparkSession.sessionState.executePlan(dataFrame.queryExecution.logical).optimizedPlan.stats.sizeInBytes
    logInfo("ByteSize: " + size)
    val sizeInMB: Double = size.toLong / (1024.0) / (1024.0)
    logInfo("MegaByteSize: " + sizeInMB)
    var numPartition: Int = (sizeInMB / (1024.0)).ceil.toInt
    logInfo("Partitions: " + numPartition)

    if (numPartition <= 0) {
      numPartition = 1
    } else if (numPartition > 100) {
      numPartition = 100
    }

    numPartition
  }
}
