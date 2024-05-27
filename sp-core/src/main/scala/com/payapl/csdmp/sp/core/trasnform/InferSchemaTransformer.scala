package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import com.paypal.csdmp.sp.utils.SchemaUtils.RichSchema
import org.apache.spark.sql.execution.datasources.csv2.CSVInferSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class InferSchemaTransformer(name: String,
                                  dataFrameName: String,
                                  timestampFormat: String = "yyyy-MM-dd HH:mm:ss[.SSS]",
                                  outputDataFrameName: String
                                 ) extends Transformer {
  override def run()(implicit sparkSession: SparkSession): Unit = {
    val sourceDataFrame: DataFrame = sparkSession.table(dataFrameName)
    if (sourceDataFrame.isEmpty) {
      logInfo(s"dataFrame $dataFrameName is empty, the application will be closed")
      System.exit(0)
    }
    val res: DataFrame = adjustSchema(sourceDataFrame)
    res.createOrReplaceTempView(outputDataFrameName)
  }

  def adjustSchema(df: DataFrame): DataFrame = {
    val schema: StructType = CSVInferSchema.inferFromDataFrame(df, Map("inferSchema" -> "true", "timestampFormat" -> timestampFormat))
    val cols: Array[Column] = df.schema.fields.zip(schema.fields).map {
      case (f1, f2) =>
        if (f1.dataType != f2.dataType) {
          col(f1.name).cast(f2.dataType).as(f1.name)
        } else {
          col(f1.name)
        }
    }
    val resFrame: DataFrame = df.select(cols: _*)
    logInfo(s"after re-inferred the schema DDL is : ${resFrame.schema.toRdsDDL}")
    resFrame
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
