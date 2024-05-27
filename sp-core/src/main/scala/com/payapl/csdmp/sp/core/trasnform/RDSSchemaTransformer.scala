package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.config.SpContext
import com.payapl.csdmp.sp.core.Transformer
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

case class RDSSchemaTransformer(name: String,
                                dataFrameName: String,
                                primaryKey: String,
                                eventTime: Option[String],
                                op: Option[String],
                                dt: Option[String],
                                location: Option[String],
                                source: Option[String],
                                traceInfo: Option[String],
                                encryptColumns: Option[String],
                                jarPath: Option[String],
                                className: Option[String],
                                outputDataFrameName: String
                               ) extends Transformer {

  override def run()(implicit sparkSession: SparkSession): Unit = {
    val dataFrame: DataFrame = sparkSession.table(dataFrameName)
    val df: DataFrame = encryptColumns.map(cols => encryptData(dataFrame, cols)).getOrElse(dataFrame)
    df.select(
      concat(buildPK(primaryKey, "|"): _*).as("pk"),
      to_json(struct(dataFrame.columns.map(x => col(quoteIdentifier(x)).as(x.toLowerCase)): _*)).as("data"),
      eventTime.map(col(_)).getOrElse(lit(null)).cast("timestamp").as("event_ts"),
      op.map(col(_)).getOrElse(lit(null)).cast("string").as("op"),
      lit(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).cast("timestamp").as("ingst_ts"),
      location.map(lit(_)).getOrElse(lit(dataFrame.sparkSession.conf.get("location", null))).cast("string").as("loc"),
      source.map(lit(_)).getOrElse(lit(dataFrame.sparkSession.conf.get("source"))).cast("string").as("src"),
      traceInfo.map(parseParameter(_)).getOrElse(lit(dataFrame.sparkSession.conf.get("trace_info"))).cast("string").as("trace_info"),
      source.map(col(_)).getOrElse(lit(SpContext.getBatchId)).as("dt")
    )
  }

  def parseParameter(paraName: String): Column = {
    paraName.split("#") match {
      case Array(column) => col(column)
      case Array(constSymbol, constValue) if constSymbol.equalsIgnoreCase("const") => lit(constValue)
      case _ => throw new IllegalArgumentException("traceInfo format should be in format (const#value or columnName)")
    }
  }

  def encryptData(dataFrame: DataFrame, columns: String): DataFrame = {
    var df = dataFrame
    require(jarPath.isDefined && className.isDefined, s"jarPath and classname should be provided")
    dataFrame.sparkSession.sql(s"add jar ${jarPath.get}")
    dataFrame.sparkSession.sql(s"""create temporary function encryption as '${className.get}'""")
    columns.trim.split(",").map(_.trim).foreach(col => {
      df = df.withColumn(col, expr(s"encryptFunc($col)"))
    })
    df
  }

  def buildPK(primaryKey: String, delimiter: String): List[Column] = {
    val cols: ListBuffer[Column] = scala.collection.mutable.ListBuffer[Column]()
    var first = true
    for (x <- primaryKey.split(",")) {
      val column = when(col(quoteIdentifier(x)).isNull, lit("-")).otherwise((col(quoteIdentifier(x))))
      if (first) {
        cols += column
        first = false
      } else {
        cols += lit(delimiter)
        cols += column
      }
    }
    cols.toList
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
