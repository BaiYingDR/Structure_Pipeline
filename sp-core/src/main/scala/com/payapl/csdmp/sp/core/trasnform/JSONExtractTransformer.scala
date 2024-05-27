package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.config.SpContext
import com.payapl.csdmp.sp.core.Transformer
import com.paypal.csdmp.sp.utils.GcsUtils
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @param name
 * @param dataFrameName
 * @param jsonColumnName
 * @param schemaName
 * @param schemaDDL
 * @param addIngestionTime
 * @param outputDataFrameName
 */
case class JSONExtractTransformer(name: String,
                                  dataFrameName: String,
                                  jsonColumnName: String,
                                  schemaName: Option[String],
                                  schemaDDL: Option[String],
                                  addIngestionTime: Option[Boolean],
                                  outputDataFrameName: String
                                 ) extends Transformer {
  override def run()(implicit sparkSession: SparkSession): Unit = {
    val dataFrame: DataFrame = sparkSession.table(dataFrameName)
    val schema: StructType = schemaDDL.map(ddl => {
      logInfo(s"load schema from input schemaDDL: $ddl")
      StructType.fromDDL(ddl)
    }).orElse(schemaName.map(readSchema(_)))
      .getOrElse(throw new IllegalArgumentException("schemaName or schemeDDL should provided at least one..."))
    //build column list、
    val cols = new ArrayBuffer[Column]()
    cols += col("json.*")
    addIngestionTime.foreach(b => if (b) cols += lit(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).cast("timestamp").as("ingst_ts"))
    //extract all the business columns
    val res: DataFrame = dataFrame.withColumn("json", from_json(col(jsonColumnName), schema))
      .select(cols: _*)
    res.createOrReplaceTempView(outputDataFrameName)
  }

  def readSchema(schemaName: String): StructType = {
    val path = s"gs://${SpContext.getBucketName}/schema/${schemaName.replace(".", "_")}"
    logInfo(s"load schema file from $path")
    StructType.fromDDL(GcsUtils.read(path))
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
