package com.payapl.csdmp.sp.core.output.hdfs

import com.paypal.csdmp.sp.exception.InvalidArgumentException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import com.payapl.csdmp.sp.core.Writer
import org.apache.spark.sql.{DataFrame, functions}

case class HdfsBatchWriter(name: String,
                           dataFrameName: String,
                           hdfsPath: Option[String],
                           fileSaveFormat: Option[String],
                           dynamicPartition: Option[Boolean],
                           partitionColumn: Option[String],
                           tableName: Option[String],
                           repartitionNumber: Option[Int],
                           saveMode: Option[String]
                          ) extends Writer {


  override def write(dataFrame: DataFrame): Unit = {

    val mode: String = saveMode.getOrElse("overwrite")
    val repartition: Int = repartitionNumber.getOrElse(1)

    (dynamicPartition, partitionColumn) match {
      case (Some(dynamicPartition), Some(partitionColumn)) =>
        if (dynamicPartition && partitionColumn.nonEmpty) {
          val partitionColumnList: Array[String] = partitionColumn.split("").map(elem => elem.mkString)
          for (col <- partitionColumnList) {
            if (col.split("=").length > 1) {
              throw new InvalidArgumentException("When using dynamic partition,do not set specific value: " + col)
            }
            if (!dataFrame.columns.contains(col)) {
              throw new InvalidArgumentException("there's no partition column " + col + " in dataFrame")
            }
          }

          logInfo("Writing into dynamic partition, partitioned by " + partitionColumn)
          writeHdfsDynamicPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition, partitionColumnList)

          dataFrame.sparkSession.sql("msck repair table " + tableName.get)
          logInfo("msck repair table finished, all hdfs write finished!")

        } else if (dynamicPartition && partitionColumn.isEmpty) {
          throw new InvalidArgumentException("Missing partitionColumn")

        } else if (!dynamicPartition && partitionColumn.nonEmpty) {
          val partitionColumnList: Array[String] = partitionColumn.split(",,").map(elem => elem.mkString)
          for (col <- partitionColumnList) {
            if (col.split("=").length <= 1) {
              throw new InvalidArgumentException("Missing specific value of partition: " + col)
            }
          }

          logInfo("Writing into fixed partition")
          writeHdfsFixedPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition, partitionColumnList)

          dataFrame.sparkSession.sql("msck repair table " + tableName.get)
          logInfo("msck repair table finished, all hdfs write finished!")


        } else {
          logInfo("Writing without partition")
          writeHdfsWithoutPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition)
        }
      case (Some(dynamicPartition), None) =>
        if (dynamicPartition) {
          throw new InvalidArgumentException("Missing partitionColumn")
        } else {
          logInfo("Writing without partition")
          writeHdfsWithoutPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition)
        }
      case (None, Some(partitionColumn)) => {
        if (partitionColumn.isEmpty) {
          logInfo("Writing without partition")
          writeHdfsWithoutPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition)
        } else {
          val partitionColumnList: Array[String] = partitionColumn.split(",").map(elem => elem.mkString)
          for (col <- partitionColumnList) {
            if (col.split("=").length > 1) {
              throw new InvalidArgumentException("Missing specific value of partition: " + col)
            }
          }

          logInfo("Writing into fixed partition")
          writeHdfsFixedPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition, partitionColumnList)

          dataFrame.sparkSession.sql("msck repair table " + tableName.get)
          logInfo("msck repair table finished, all hdfs write finished")

        }
      }
      case (None, None) =>
        logInfo("Writing without partition")
        writeHdfsWithoutPartition(dataFrame, fileSaveFormat.get, mode, hdfsPath.get, repartition)
    }
  }


}

private def writeHdfsWithoutPartition (dataFrame: DataFrame, format: String, mode: String, path: String, rpNum: Int): Unit = {
  if ("parquet".equalsIgnoreCase (format) ) {
  dataFrame.repartition (rpNum).write.mode (mode).parquet (path)
  } else {
  dataFrame.repartition (rpNum).write.mode (mode).save (path)
  }
  }

  private def writeHdfsFixedPartition (dataFrame: DataFrame, format: String, mode: String, path: String, rpNum: Int, partitionColumnList: Array[String] ): Unit = {
  var df: DataFrame = dataFrame
  var finalHdfsPath: String = path

  for (partition <- partitionColumnList) {
  val oneKeyWithOneValue: Array[String] = partition.split ("=")
  //add new Column,,they're partition column.
  df = df.withColumn (oneKeyWithOneValue (0), functions.lit (oneKeyWithOneValue (1) ) )
  finalHdfsPath = finalHdfsPath + "/" + partition
  }

  logInfo ("finalHdfsPath: " + finalHdfsPath)
  logInfo ("DataFrame schema is ")
  df.printSchema ()
  df.sparkSession.sparkContext.hadoopConfiguration.set ("parquet.enable.dictionary", "false")

  if ("parquet".equalsIgnoreCase (format) ) {
  dataFrame.repartition (rpNum).write.mode (mode).parquet (finalHdfsPath)
  } else {
  dataFrame.repartition (rpNum).write.mode (mode).save (finalHdfsPath)
  }
  }

  private def writeHdfsDynamicPartition (dataFrame: DataFrame, format: String, mode: String, path: String, rpNum: Int, partitionColumnList: Array[String] ): Unit = {
  if ("overwrite".equalsIgnoreCase (mode) ) {
  dataFrame.sparkSession.conf.set ("spark.sql.source.partitionOverwriteMode", "dynamic")
  }

  if ("parquet".equalsIgnoreCase (format) ) {
  dataFrame.repartition (rpNum).write.partitionBy (partitionColumnList: _*).mode (mode).parquet (path)
  } else {
  dataFrame.repartition (rpNum).write.partitionBy (partitionColumnList: _*).mode (mode).save (path)
  }

  if ("overwrite".equalsIgnoreCase (mode) ) {
  dataFrame.sparkSession.conf.set ("spark.sql.source.partitionOverwriteMode", "static")
  }

  }

  private def mkdirForHdfsDynamicWrite (dataFrame: DataFrame, path: String, partitionColumnList: Array[String] ): Unit = {
  val hadoopConf: Configuration = dataFrame.sparkSession.sparkContext.hadoopConfiguration
  val fs: FileSystem = FileSystem.get (hadoopConf)

  val partitionColumnHead: String = partitionColumnList.head
  val valueArray: Array[Row] = dataFrame.select (dataFrame.col (partitionColumnHead) ).distinct ().collect ()
  for (v <- valueArray) {
  val p: String = path + "/" + partitionColumnHead + "=" + v.mkString ("")
  logInfo ("mkdir: " + p)
  val mkdirPath = new Path (p)
  fs.mkdirs (mkdirPath)
  }


  }
  }
