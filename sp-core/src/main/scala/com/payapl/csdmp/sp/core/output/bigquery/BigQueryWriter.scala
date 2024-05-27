package com.payapl.csdmp.sp.core.output.bigquery

import com.google.api.gax.paging.Page
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.consts.Constant.DATASOURCE_FORMAT_BIGQUERY
import com.paypal.csdmp.sp.exception.DataNotMatchException
import org.apache.spark.sql.DataFrame

import java.util.UUID
import scala.collection.mutable

/**
 *
 * @param name
 * @param projectName
 * @param dataSetName
 * @param tmpProjectName
 * @param tmpDataSetName
 * @param tableName
 * @param bucketName
 * @param dataFrameName
 * @param partitionColumn
 * @param materializationDataset
 */
case class BigQueryWriter(name: String,
                          projectName: String,
                          dataSetName: String,
                          tmpProjectName: String = "pypl-edw",
                          tmpDataSetName: String,
                          tableName: String,
                          bucketName: Option[String],
                          dataFrameName: String,
                          partitionColumn: Option[String],
                          materializationDataset: String = "pp_cs_stage",
                         ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {

    val bucket: String = bucketName.getOrElse(sys.env("GOOGLE_BUCKET"))
    dataFrame.sparkSession.conf.set("temporaryGcsBucket", bucket)
    dataFrame.sparkSession.conf.set("viewsEnables", "ture")
    dataFrame.sparkSession.conf.set("materializationProject", projectName)
    dataFrame.sparkSession.conf.set("materializationDataset", materializationDataset)

    val targetTableName = s"`$projectName.$dataSetName.$tableName`"
    logInfo(s"Bigquery Output target table: $targetTableName")
    val uuidForTmpTable: String = UUID.randomUUID().toString.replace("-", "")
    val tmpTableNameForWrite: String = s"$tmpProjectName.$tmpDataSetName.${dataFrameName}_${uuidForTmpTable}"
    val tmpTableName = s"`$tmpTableNameForWrite`"
    logInfo(s"Write source dataframe into tmp table: $tmpTableName")

    //Write source dataframe into tmp table
    dataFrame.write.mode("overwrite")
      .format(DATASOURCE_FORMAT_BIGQUERY)
      .option("table", tmpTableNameForWrite)
      .save()

    val bigqueryConn: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val columnNameAndTypeMap: mutable.Map[String, String] = BigQueryUtils.getColumnNameAndType(bigqueryConn, TableId.of(projectName, dataSetName, tableName))
    logInfo("ColumnNameAndTypeMap in target table: " + columnNameAndTypeMap)

    val tmpColumnNameAndTypeMap: mutable.Map[String, String] = BigQueryUtils.getColumnNameAndType(bigqueryConn, TableId.of(tmpProjectName, tmpDataSetName, tmpDataSetName + "_" + uuidForTmpTable))
    logInfo("ColumnNameAndTypeMap in temp table: " + tmpColumnNameAndTypeMap)

    // check if the number of column is same
    if (columnNameAndTypeMap.toList.size != tmpColumnNameAndTypeMap.toList.size) {
      logError("tmp table and target table's column number don't match, please check the schema information print above")
      throw new DataNotMatchException("tmp table and target table's column number don't match, please check the schema information print above")
    }

    try {
      partitionColumn match {
        case Some(partitionColumn) =>
          logInfo("overwrite related partition in the target table")
          BigQueryUtils.overwritePartitionWithTransaction(bigqueryConn, partitionColumn, tmpTableName, targetTableName, columnNameAndTypeMap, tmpColumnNameAndTypeMap, dataFrame)
        case None =>
          logInfo("overwrite all data in the target table")
          BigQueryUtils.overwriteTableWithTransaction(bigqueryConn, tmpTableName, targetTableName, columnNameAndTypeMap, tmpColumnNameAndTypeMap, dataFrame)
      }
    } catch {
      case ex: Exception =>
        logError("Exception when using bigquery writer!", ex)
        processOfWriteFailed(bucketName.get, name)
        throw ex
    } finally {
      logInfo("drop tmp table: " + tmpTableName)
      BigQueryUtils.executeSQL("drop table " + tmpTableName, bigqueryConn)
    }

    logInfo("Bigquery writer finished")

  }

  private def processOfWriteFailed(bucketName: String, name: String): Unit = {
    val storage: Storage = StorageOptions.getDefaultInstance.getService
    val blobs: Page[Blob] = storage.list(bucketName, BlobListOption.prefix(name + "/"), BlobListOption.currentDirectory())
    import scala.collection.JavaConversions._
    for (blob <- blobs.iterateAll) {
      blob.delete()
      logInfo(blob.getName + "is deleted")
    }
  }

}
