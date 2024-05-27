package com.payapl.csdmp.sp.core.input.bigquery

import com.payapl.csdmp.sp.core.Reader
import com.paypal.csdmp.sp.consts.Constant.DATASOURCE_FORMAT_BIGQUERY
import com.paypal.csdmp.sp.utils.parseContextInQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

case class BigQueryReader(name: String,
                          outputDataFrame: Option[String],
                          projectName: String = "pypl-edw",
                          datasetName: Option[String],
                          tableName: Option[String],
                          column: Option[String],
                          filter: Option[String],
                          sqlText: Option[String],
                          materializationDataset: String = "pp_scratch",
                          options: Option[Map[String, String]]
                         ) extends Reader {

  override def read()(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.conf.set("viewEnabled", "true")
    sparkSession.conf.set("materializationDataset", materializationDataset)
    sparkSession.conf.set("materializationProject", projectName)

    sqlText match {
      case Some(sql) =>
        val query: String = parseContextInQuery(sql, sparkSession)
        val randomQuery: String = addRandomClauseIntoSQL(sql)
        logInfo("the Query with random query is " + randomQuery)
        try {
          sparkSession.read.format(DATASOURCE_FORMAT_BIGQUERY).load(randomQuery)
        } catch {
          case e: Exception =>
            sparkSession.read.format(DATASOURCE_FORMAT_BIGQUERY).load(query)
        }
      case None =>
        val table: String = projectName + "." + datasetName.get + "." + tableName.get
        logInfo("Table is " + table)
        val tableDataFrame: DataFrame = sparkSession.read.format(DATASOURCE_FORMAT_BIGQUERY).load(table)
        column match {
          case Some(column) =>
            val columnArray: Array[String] = column.split(",")
            filter match {
              case Some(filter) =>
                logInfo("Has Column AND Filter")
                tableDataFrame.where(filter).select(columnArray.head, columnArray.tail: _*)
              case None =>
                logInfo("Has Column BUT None Filter")
                tableDataFrame.select(columnArray.head, columnArray.tail: _*)
            }
          case None =>
            filter match {
              case Some(filter) =>
                logInfo("Has None Column BUT Have Filter")
                tableDataFrame.where(filter)
              case None =>
                logInfo("Has None Column AND None Filter")
                tableDataFrame
            }
        }
    }
  }

  def addRandomClauseIntoSQL(sql: String): String = {
    val alias: Char = Random.alphanumeric.filter(_.isLetter).head
    "select " + alias + ".* from ( " + sql + " ) " + alias
  }
}
