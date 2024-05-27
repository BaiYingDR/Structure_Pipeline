package com.payapl.csdmp.sp.core.output.hdfs

import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.exception.MissingArgumentException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger


case class HdfsStreamWriter(name: String,
                            dataFrameName: String,
                            hdfsPath: Option[String],
                            fileSaveFormat: Option[String],
                            partitionColumn: Option[String],
                            tableName: Option[String],
                            checkPointLocation: Option[String]
                           ) extends Writer {


  override def write(dataFrame: DataFrame): Unit = {

    dataFrame.sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.dictionary", "false")

    (hdfsPath, checkPointLocation, fileSaveFormat) match {
      case (Some(_hdfsPath), Some(_checkPointLocation), Some(_fileSaveFormat)) =>
        val finalHdfsPath: String = partitionColumn.get.split(",")
          .foldLeft(_hdfsPath) {
            (finalHdfsPath, partition) => finalHdfsPath + "/" + partition.mkString
          }
        logInfo(s"The final HdfsPath of hdfs sink is $finalHdfsPath")
        dataFrame
          .writeStream
          .trigger(Trigger.Once())
          .format(_fileSaveFormat)
          .option("path", finalHdfsPath)
          .option("checkpointLocation", _checkPointLocation)
          .start()

        dataFrame.sparkSession.streams.awaitAnyTermination()

      case _ =>
        throw new MissingArgumentException("The configuration of HDFSStreamWriter is invalid")
    }
  }
}
