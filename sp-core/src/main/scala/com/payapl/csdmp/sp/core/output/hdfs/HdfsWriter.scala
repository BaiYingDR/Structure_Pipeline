package com.payapl.csdmp.sp.core.output.hdfs

import com.payapl.csdmp.sp.core.Writer
import org.apache.spark.sql.DataFrame

case class HdfsWriter(name: String,
                      dataFrameName: String,
                      hdfsPath: Option[String],
                      fileSaveFormat: Option[String],
                      dynamicPartition: Option[Boolean],
                      partitionColumn: Option[String],
                      tableName: Option[String],
                      repartitionNumber: Option[Int],
                      saveMode: Option[String],
                      checkpointLocation: Option[String],
                      isStream: Boolean = false
                     ) extends Writer {
  override def write(dataFrame: DataFrame): Unit = {

    if (isStream) {
      require(hdfsPath.isDefined && checkpointLocation.isDefined && fileSaveFormat.isDefined && partitionColumn.isDefined,
        "hdfsPath,checkpointLocation,outputMode,fileSaveFormat and partitionColumn is mandatory.")

      HdfsStreamWriter(
        name,
        dataFrameName,
        hdfsPath,
        fileSaveFormat,
        partitionColumn,
        tableName,
        checkpointLocation
      ).write(dataFrame)
    } else {
      require(Option(hdfsPath).isDefined, "hdfsPath is mandatory.")
      HdfsBatchWriter(
        name,
        dataFrameName,
        hdfsPath,
        fileSaveFormat,
        dynamicPartition,
        partitionColumn,
        tableName,
        repartitionNumber,
        saveMode
      ).write(dataFrame)
    }
  }
}
