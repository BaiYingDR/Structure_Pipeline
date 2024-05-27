package com.payapl.csdmp.sp.core.input.hive

import com.payapl.csdmp.sp.core.Reader
import com.paypal.csdmp.sp.exception.MissingArgumentException
import com.paypal.csdmp.sp.utils.parseContextInQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveReader(name: String,
                      outputDataFrame: Option[String],
                      database: Option[String],
                      tableName: Option[String],
                      filter: Option[String],
                      column: Option[String],
                      sqlText: Option[String],
                      options: Option[Map[String, String]]) extends Reader {

  override def read()(implicit sparkSession: SparkSession): DataFrame = {
    sqlText match {
      case Some(sql) =>
        logInfo("has sql, will run")
        val query: String = parseContextInQuery(sql, sparkSession)
        sparkSession.sql(query)
      case None =>
        if (!database.isDefined || !tableName.isDefined) {
          throw new MissingArgumentException("database and tableName is required if sqlText is not defined!")
        }
        sparkSession.sql(s"use ${database.get}")

        val tableDataFrame: DataFrame = sparkSession.table(tableName.get)
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
}
