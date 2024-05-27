package com.payapl.csdmp.sp.core.output.file

import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import com.paypal.csdmp.sp.utils.{GcsUtils, checkEnv}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

import java.io.File

case class FileWriter(name: String,
                      dataFrameName: String,
                      path: Option[String],
                      fileName: Option[String],
                      number: Option[Int],
                      format: Option[String],
                      outputMode: Option[String],
                      options: Option[Map[String, String]],
                      dynamicPartitionColumn: Option[String],
                      fixedPartitionColumn: Option[String],
                      tableName: Option[String]
                     ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {

    logInfo(s"Format is $format,Path is $path, Option is $options, OutputMode is ${outputMode.getOrElse("overwrite")}")
    val currentPartitionColumnNum: Int = dataFrame.rdd.getNumPartitions
    val repartitionNum: Int = number.getOrElse(currentPartitionColumnNum)
    if (repartitionNum > currentPartitionColumnNum) {
      logInfo("The number more than current partition number of dataframe,  will keep current number of partition")
    }
    var writer: DataFrameWriter[Row] = dataFrame.repartition(repartitionNum).write.format(format.get).mode(outputMode.getOrElse("overwrite")).options(options.getOrElse(Map()))
    val finalPath: String = path.get
    if (finalPath.startsWith("hdfs")) {
      dynamicPartitionColumn match {
        case Some(dynamicPartitionColumn) =>
          dataFrame.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          val partitionColList: Array[String] = dynamicPartitionColumn.split(",").map(e => e.mkString)
          partitionColList.foreach(e => if (!dataFrame.columns.contains(e)) throw new InvalidArgumentException("There is no partition column " + e + " in dataframe"))
          //mkdirForHdfsDynamicWrite
          val fs: FileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
          dataFrame.select(col(partitionColList.head)).distinct().foreach(v => {
            fs.mkdirs(new Path(finalPath + "/" + partitionColList.head + "=" + v.mkString("")))
          })
          logInfo(s"Writing into dynamic partition partition by $dynamicPartitionColumn")
          writer = writer.partitionBy(partitionColList: _*)
        case None =>
          dataFrame.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
          fixedPartitionColumn match {
            case Some(fixedPartitionColumn) =>
              val partitionColumnList: Array[String] = fixedPartitionColumn.split(",").map(e => e.mkString)
              partitionColumnList.foldLeft(finalPath) {
                (finalHdfsPath, partition) => finalHdfsPath + "/" + partition.mkString
              }
              logInfo(s"Writing into fixed partition $finalPath")
              dataFrame.sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.dictionary", "false")
            case None =>
              logInfo("writing with out partition")
          }
      }
    }

    writer.save(finalPath)

    if (tableName.isDefined) {
      dataFrame.sparkSession.conf.set("msck repair table " + tableName.get)
      logInfo("msck repair table finished, all hdfs write finished")
    }
    if (fileName.isDefined) {
      var targetName: String = fileName.get
      checkEnv(finalPath) match {
        case "hdfs" =>
          if (dynamicPartitionColumn.isDefined) {
            logError("File rename is not supported on dynamic partition write.", new InvalidArgumentException("File rename is not support on dynamic partition write"))
          }
          val fs: FileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
          fs.globStatus(new Path(s"$finalPath/part*")).zipWithIndex.foreach {
            case (f, idx) =>
              if (!fs.rename(new Path(s"$finalPath/${f.getPath.getName}"), new Path(s"$finalPath/$targetName"))) {
                logError("Fail to rename file", new RuntimeException("Fail to rename file"))
              }
              targetName = fileName.get + "-" + idx.toString
          }

        case "gcs" =>
          val files: List[String] = GcsUtils.listDir(finalPath).filter(f => FilenameUtils.getName(f).startsWith("part"))
          if (files.size == 1) {
            val fileName: String = files.head
            GcsUtils.copy(fileName, s"$finalPath/$targetName", true)
            logInfo(s"Copied object from object $fileName to $finalPath/$targetName")
          } else if (files.size > 1) {
            files.zipWithIndex.foreach {
              case (f, idx) =>
                import com.paypal.csdmp.sp.utils.StringUtils._
                val extension: String = FilenameUtils.getExtension(fileName.get)
                targetName = if (extension.isEmpty) {
                  s"${fileName.get}-${idx + 1}"
                } else {
                  s"${fileName.get.removeSuffix(extension)}-${idx + 1}.$extension"
                }
                GcsUtils.copy(f, s"$finalPath/$targetName", true)
                logInfo(s"Copied object from object $f to $finalPath/$targetName")
            }
          }

        case "file" =>
          val folder = new File(finalPath)
          folder.listFiles.filter(_.isFile).filter(_.getName.startsWith("part"))
            .zipWithIndex.foreach {
            case (f, idx) =>
              if (!f.renameTo(new File(s"$finalPath/$targetName"))) {
                logError("Failed to rename file", new RuntimeException("Fail to rename file"))
                targetName = fileName.get + "-" + idx.toString
              }
          }
      }
    }
  }
}
