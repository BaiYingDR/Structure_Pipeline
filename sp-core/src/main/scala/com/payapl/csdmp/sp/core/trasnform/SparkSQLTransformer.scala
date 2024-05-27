package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkSQLTransformer(name: String,
                               sqlText: String,
                               outputDataFrameName: String
                              ) extends Transformer {

  override def run()(implicit sparkSession: SparkSession): Unit = {
    var sql: String = sqlText
    if (sql.contains("ctx_")) {
      val map: Map[String, String] = sparkSession.conf.getAll
      map.keySet.filter(_.startsWith("ctx_")).foreach(k => sql = sql.replaceAll(k, map(k)))
    }

    try {
      val sqlArray: Array[String] = sql.split(";")
      for (i <- 0 until sqlArray.length - 1) {
        logInfo(s"Execute the sql ${sqlArray(i)}")
        sparkSession.sqlContext.sql(sqlArray(i))
      }

      logInfo(s"Execute the last sql: ${sqlArray.last}")
      val newDF: DataFrame = sparkSession.sqlContext.sql(sqlArray.last)
      newDF.createOrReplaceTempView(outputDataFrameName)

      logInfo(
        s"""
           |# StepName        :$name
           |# SQL             :$sqlText
           |# OutputDataFrame :$outputDataFrameName
           |# OutputDfSchema  :
           |""".stripMargin)
      newDF.printSchema()
    } catch {
      case e: Exception => {
        log.error(s"Exception occurs on step $name when run sql query: $sqlText", e)
        throw e
      }
    }
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
