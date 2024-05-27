package com.payapl.csdmp.sp.core.output.bigquery

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import com.payapl.csdmp.sp.core.Writer
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import com.paypal.csdmp.sp.utils.SparkFunctionUtils.{SORT_FUNCTION_MAP, addSorts, getWindow}
import com.paypal.csdmp.sp.consts.Constant.DATASOURCE_FORMAT_BIGQUERY
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.util.UUID

case class BigQueryMergeWriter(name: String,
                               dataFrameName: String,
                               projectName: String,
                               dataSetName: String,
                               tableName: String,
                               bucketName: Option[String],
                               materializationDataset: String,
                               pkColumn: Option[String],
                               valueColumn: Option[String],
                               sortingColumn: Option[Map[String, String]],
                               mergeCompareColumn: Option[String],
                               createTsColumn: Option[String],
                               updateTsColumn: Option[String],
                               tmpProjectName: String,
                               tmpDataSetName: String,
                               customizedMergeSql: Option[String],
                               useImplicitConversion: Boolean = false
                              ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {

    var sourceDF: DataFrame = dataFrame
    val sparkSession: SparkSession = dataFrame.sparkSession
    sparkSession.conf.set("materializationDataset", materializationDataset);
    sparkSession.conf.set("temporaryGcsBucket", bucketName.getOrElse(sys.env("GOOGLE_BUCKET")))

    val uuidForTmpTable: String = UUID.randomUUID().toString().replace("-", "")
    val tmpTableNameForWrite: String = tmpProjectName + "." + tmpDataSetName + "." + dataFrameName + "_" + uuidForTmpTable
    log.warn(s"Using default tmpProjectName: $tmpProjectName")
    log.warn(s"Using default tmpDataSetName: $tmpDataSetName")

    sortingColumn match {
      case Some(sortCols) =>
        val pkColumnList: Some[List[String]] = Some(pkColumn.get.split(",").map(i => i.mkString.trim).toList)

        val sorts: Option[List[Column]] = Option(sortCols.map(col => {
          val sortType: String = col._2
          if (SORT_FUNCTION_MAP.contains(sortType)) {
            SORT_FUNCTION_MAP(sortType)(col._1)
          } else {
            throw new InvalidArgumentException("Invalid Sort Type:" + sortType + ", Please enter invalid sort type!")
          }
        }).toList)

        var window: WindowSpec = getWindow(pkColumnList, None)
        window = addSorts(window, sorts)
        sourceDF = sourceDF.withColumn("Rank", functions.row_number().over(window))
        sourceDF = sourceDF.where("Rank = 1").drop("Rank")

      case + =>
        logInfo("Will not deduplicate source df")
    }

    logInfo("Write source df into tmp table" + tmpTableNameForWrite)
    sourceDF.write.mode("overwrite")
      .format(DATASOURCE_FORMAT_BIGQUERY)
      .option("table", tmpTableNameForWrite)
      .save()

    val bigqueryConn: BigQuery = BigQueryOptions.getDefaultInstance.getService
    logInfo("Bigquery connection here:" + bigqueryConn)

    val targetTableId: TableId = TableId.of(projectName, dataSetName, tableName)
    val columnNameAndTypeMap: Map[String, String] = BigQueryUtils.getColumnNameAndType(bigqueryConn, targetTableId)
    logInfo("Schema inn target table:" + columnNameAndTypeMap.toList)


    val tmpTableId: TableId = TableId.of(tmpProjectName, tmpDataSetName, dataFrameName + "_" + uuidForTmpTable)
    val tmpColumnNameAndTypeMap: Map[String, String] = BigQueryUtils.getColumnNameAndType(bigqueryConn, tmpTableId)
    logInfo("Schema inn temp table:" + tmpColumnNameAndTypeMap.toList)

    // make the merge sql
    val tmpTableName: String = "`" + tmpTableNameForWrite + "`"
    val targetTableName: String = "`" + projectName + "." + dataSetName + "." + tableName + "`"

    val temptableAlias: String = tmpProjectName + "." + tmpDataSetName + "." + dataFrameName + "_temp"

    val mergeSQL: String = customizedMergeSql match {
      case Some(_customizedMergeSql) =>
        _customizedMergeSql.replace(temptableAlias, tmpTableNameForWrite)
      case None =>
        BigQueryUtils.generateMergeSql(pkColumn,
          columnNameAndTypeMap,
          tmpColumnNameAndTypeMap,
          tmpTableName,
          targetTableName,
          valueColumn,
          createTsColumn,
          updateTsColumn,
          mergeCompareColumn,
          useImplicitConversion
        )
    }

    logInfo("The merge sql is " + mergeSQL)
    //execute the merge sql
    BigQueryUtils.executeSQL(mergeSQL, bigqueryConn)

    //drop the temporary table
    BigQueryUtils.executeSQL("drop table " + tmpTableName, bigqueryConn)


  }

}
