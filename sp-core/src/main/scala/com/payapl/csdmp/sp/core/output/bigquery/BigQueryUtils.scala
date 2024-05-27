package com.payapl.csdmp.sp.core.output.bigquery

import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery._
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.consts.Constant
import org.apache.spark.sql.DataFrame

import java.io
import java.util.UUID
import scala.collection.JavaConversions._
import scala.collection.mutable

object BigQueryUtils extends Logging {

  def getColumnNameAndType(bigqueryConn: BigQuery, tableId: TableId): mutable.Map[String, String] = {
    val tableFields: FieldList = bigqueryConn.getTable(tableId).getDefinition[StandardTableDefinition]().getSchema.getFields
    val columnNameAndTypeMap = new mutable.HashMap[String, String]()
    tableFields.toList.foreach(f =>
      columnNameAndTypeMap.put(f.getName.toLowerCase(), f.getType.name())
    )
    columnNameAndTypeMap
  }


  def executeSQL(sql: String, bigQuery: BigQuery) = {
    logInfo("BigQuery standard sql to be executed: " + sql)
    val startTimeMills: Long = System.currentTimeMillis()

    val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build()
    val jobId: JobId = JobId.of(UUID.randomUUID().toString)
    val job: Job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor()

    val endTimeMills: Long = System.currentTimeMillis()
    logInfo("Bigquery standard SQL execution spends time: " + ((endTimeMills - startTimeMills) / 1000).toString + " seconds")

    if (job == null) {
      throw new RuntimeException(" job no longer exists")
    } else if (job.getStatus.getError != null) {
      throw new RuntimeException(job.getStatus.getError.toString)
    }

    val statistics: QueryStatistics = job.getStatistics[QueryStatistics]
    Option(statistics.getDmlStats).foreach(dmlStats => {
      logInfo(s"DeletedRowCount is ${dmlStats.getDeletedRowCount}")
      logInfo(s"UpdatedRowCount is ${dmlStats.getUpdatedRowCount}")
      logInfo(s"InsertedRowCount is ${dmlStats.getInsertedRowCount}")
    })

    val mergeResult: TableResult = job.getQueryResults()
    logInfo("Get job's result")

    for (row <- mergeResult.iterateAll) {
      logInfo("Read record: " + row)
    }
  }

  def overwriteTableWithTransaction(bigqueryConn: BigQuery, tmpTableName: String, targetTableName: String,
                                    columnNameAndTypeMap: mutable.Map[String, String], tmpColumnNameAndTypeMap: mutable.Map[String, String],
                                    dataFrame: DataFrame): Unit = {
    val deleteQuery = s"DELETE FROM $targetTableName where True"

    //generate insert query
    //check if the data type is same,"DATETIME","TIME","BIGNUMERIC" are exception due to spark to BQ

    val selectCondition: String = dataFrame.schema.map(s =>
      if (columnNameAndTypeMap(s.name.toLowerCase).equalsIgnoreCase(tmpColumnNameAndTypeMap(s.name.toLowerCase))) {
        s.name
      } else if (List("DATETIME", "TIME", "BIGNUMERIC").contains(columnNameAndTypeMap(s.name.toLowerCase).toUpperCase())) {
        logInfo(s"For Column ${s.name}, ${tmpColumnNameAndTypeMap(s.name.toLowerCase())} dataType in tmp table, ${columnNameAndTypeMap(s.name.toLowerCase())} in target table, will do explicit cast")
        "cast(" + s.name.toLowerCase + " as " + columnNameAndTypeMap(s.name.toLowerCase()) + ")"
      } else {
        logWarning(s"For Column ${s.name}, ${tmpColumnNameAndTypeMap(s.name.toLowerCase())} dataType in tmp table, ${columnNameAndTypeMap(s.name.toLowerCase())} in target table, will do implicit conversion!")
        s.name
      }
    ).mkString(",")

    val insertQuery = s"INSERT INTO $targetTableName select $selectCondition from $tmpTableName;"

    val transactionalFullQuery: String =
      s"""BEGIN TRANSACTION
         |
         |$deleteQuery
         |
         |$insertQuery
         |
         |COMMIT TRANSACTION;
         |""".stripMargin
    executeSQL(transactionalFullQuery, bigqueryConn)
  }

  def overwritePartitionWithTransaction(bigQueryConn: BigQuery, partitionColumn: String, tmpTableName: String, targetTableName: String,
                                        columnNameAndTypeMap: mutable.Map[String, String], tmpColumnNameAndTypeMap: mutable.Map[String, String],
                                        dataFrame: DataFrame): Unit = {
    val partitionValueList: List[String] = dataFrame.select(partitionColumn).distinct()
      .filter(row => row != null && row.length > 0).collect().map(_ (0).toString).toList
    if (partitionValueList.isEmpty) {
      logInfo(s"There are no partition on partition column $partitionColumn on source data, will skip this write step")
      return
    }
    logInfo("These partition will be overwritten: " + partitionValueList)

    //generate delete query
    val numberTypeList = List("INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC")
    val partitionListWhereCondition: String = if (numberTypeList.contains(columnNameAndTypeMap(partitionColumn.toLowerCase).toUpperCase)) {
      partitionValueList.mkString(",")
    } else {
      partitionValueList.map(s => s"'$s'").mkString(",")
    }

    val deleteQuery = s"DELETE FROM $targetTableName WHERE $partitionColumn in ($partitionListWhereCondition)"

    val selectCondition: String = dataFrame.schema.map(s =>
      if (columnNameAndTypeMap(s.name.toLowerCase).equalsIgnoreCase(tmpColumnNameAndTypeMap(s.name.toLowerCase))) {
        s.name
      } else if (List("DATETIME", "TIME", "BIGNUMERIC").contains(columnNameAndTypeMap(s.name.toLowerCase).toUpperCase)) {
        logInfo(s"For column ${s.name}, ${tmpColumnNameAndTypeMap(s.name.toLowerCase)} dataType in tmp table, ${columnNameAndTypeMap(s.name.toLowerCase)} in target table, will do explicit cast!")
        "cast(" + s.name.toLowerCase + " as " + columnNameAndTypeMap(s.name.toLowerCase) + "）"
      } else {
        logWarning(s"For column ${s.name}, ${tmpColumnNameAndTypeMap(s.name.toLowerCase)} dataType in tmp table, ${columnNameAndTypeMap(s.name.toLowerCase)} in target table, will do implicit cast!")
        s.name
      }
    ).mkString(",")

    val insertQuery = s"INSERT INTO $targetTableName select $selectCondition from $tmpTableName;"

    val transactionalFullQuery: String =
      s"""BEGIN TRANSACTION
         |
         |$deleteQuery
         |
         |$insertQuery
         |
         |COMMIT TRANSACTION;
         |""".stripMargin
    executeSQL(transactionalFullQuery, bigQueryConn)
  }


  def generateMergeSql(pkColumn: Option[String],
                       columnNameAndTypeMap: Map[String, String],
                       tmpColumnNameAndTypeMap: Map[String, String],
                       tmpTableName: String,
                       targetTableName: String,
                       valueColumn: Option[String],
                       createTsColumn: Option[String],
                       updateTsColumn: Option[String],
                       mergeCompareColumn: Option[String],
                       useImplicitConversion: Boolean): String = {
    var activeCols: String = pkColumn.get.toLowerCase.replaceAll("\\s", "")
    if (valueColumn.isDefined) {
      activeCols = activeCols + "," + valueColumn.get.toLowerCase
    }

    var matchedValues: String = activeCols.split(",").map(x => x.trim).map(x =>
      if (useImplicitConversion) {
        if (List("DATETIME", "TIME", "BIGNUMERIC").contains(columnNameAndTypeMap(x).toUpperCase)) {
          logWarning(s"Due to the type of column $x is not match target type ${columnNameAndTypeMap(x)}, will cast the type $x ->${columnNameAndTypeMap(x)}")
          x + "= cast(" + tmpTableName + "." + x + " as " + columnNameAndTypeMap(x) + ")"
        } else {
          x + "=" + tmpTableName + "." + x
        }
      } else {
        if (!columnNameAndTypeMap(x).equalsIgnoreCase(tmpColumnNameAndTypeMap(x))) {
          logWarning(s"Due to the type of column $x is not match target type ${columnNameAndTypeMap(x)}, will cast the type $x ->${columnNameAndTypeMap(x)}")
          x + "= cast(" + tmpTableName + "." + x + " as " + columnNameAndTypeMap(x) + ")"
        } else {
          x + "=" + tmpTableName + "." + x
        }
      }).mkString(",")


    // not matched values
    var notMatchedInsertFields: String = activeCols
    var notMatchedInsertValues: String = notMatchedInsertFields.split(",").map(x => x.trim).map(x =>
      if (useImplicitConversion) {
        if (List("DATETIME", "TIME", "BIGNUMERIC").contains(columnNameAndTypeMap(x).toUpperCase)) {
          logWarning(s"Due to the type of column $x is not match target type ${columnNameAndTypeMap(x)}, will cast the type $x ->${columnNameAndTypeMap(x)}")
          "cast(" + tmpTableName + "." + x + " as " + columnNameAndTypeMap(x) + ")"
        } else {
          tmpTableName + "." + x
        }
      } else {
        if (!columnNameAndTypeMap(x).equalsIgnoreCase(tmpColumnNameAndTypeMap(x))) {
          logWarning(s"Due to the type of column $x is not match target type ${columnNameAndTypeMap(x)}, will cast the type $x ->${columnNameAndTypeMap(x)}")
          "cast(" + tmpTableName + "." + x + " as " + columnNameAndTypeMap(x) + ")"
        } else {
          tmpTableName + "." + x
        }
      }
    ).mkString(",")

    //createTsColumn
    if (createTsColumn.isDefined && !createTsColumn.get.equals("false")) {
      notMatchedInsertFields = notMatchedInsertFields + "," + createTsColumn.get
      notMatchedInsertValues = notMatchedInsertValues + ",DATETIME_TRUNC(CURRENT_DATE(),second)"
    }

    //updateTsColumn
    if (updateTsColumn.isDefined && !updateTsColumn.get.equals("false")) {
      matchedValues = matchedValues + ", " + updateTsColumn.get + "=DATETIME_TRUNC(CURRENT_DATE(),second)"
      notMatchedInsertFields = notMatchedInsertFields + "," + updateTsColumn.get
      notMatchedInsertValues = notMatchedInsertValues + ",DATETIME_TRUNC(CURRENT_DATE(),second)"
    }

    // make join condition
    val onCondition: String = pkColumn.get.toLowerCase.split(",").map(x => x.trim).map(x =>
      if (!columnNameAndTypeMap(x).equalsIgnoreCase(tmpColumnNameAndTypeMap(x))) {
        logWarning(s"Due to the type PK column $x is not match target type ${columnNameAndTypeMap(x)}, will cast the type $x -> ${columnNameAndTypeMap(x)} on where condition")
        "cast( " + tmpTableName + "." + x + " as " + columnNameAndTypeMap(x) + ")=" + targetTableName + "." + x
      }
      else {
        tmpTableName + "." + x + "=" + targetTableName + "." + x
      }
    ).mkString(",")


    //make matched condition
    val updateCondition: io.Serializable =
      if (mergeCompareColumn.isDefined) {
        val mergeCols: Array[String] = mergeCompareColumn.get.split(Constant.SYMBOL_COMMA).map(_.trim)
        mergeCols.zipWithIndex
          .map {
            case (col, index) => {
              if (index == 0) {
                s"AND CAST($tmpTableName.$col AS ${columnNameAndTypeMap(col)}) > $targetTableName.$col "
              } else {
                s"(CAST($tmpTableName.$col as ${columnNameAndTypeMap(col)}) > $targetTableName.$col AND " +
                  s"${
                    mergeCols
                      .slice(0, index)
                      .map(c => s"CAST($tmpTableName.$c AS ${columnNameAndTypeMap(c)}) = $targetTableName.$c ")
                      .mkString(" AND")
                  })"
              }
            }.mkString(" OR ")
          }
      } else ""

    val mergeSQL = "merge into " + targetTableName +
      "using" + tmpTableName +
      "on" + onCondition +
      "when matched " + updateCondition +
      "then update set " + matchedValues +
      "when not matched then insert (" + notMatchedInsertFields + ")" +
      "values(" + notMatchedInsertValues + ")"

    logInfo("targetTableName: " + targetTableName)
    logInfo("onCondition: " + onCondition)
    logInfo("matchedValues: " + matchedValues)
    logInfo("notMatchedInsertFields: " + notMatchedInsertFields)
    logInfo("notMatchedInsertValues: " + notMatchedInsertValues)
    logInfo("mergeSQL: " + mergeSQL)
    mergeSQL
  }
}


