package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

/**
 *
 * @param name
 * @param joinType
 * @param condition
 * @param dataFrameName
 * @param outputDataFrameColumns
 * @param outputDataFrameName
 */
case class JoinTransformer(name: String,
                           joinType: String,
                           condition: String,
                           dataFrameName: String,
                           outputDataFrameColumns: String,
                           outputDataFrameName: String
                          ) extends Transformer {


  override def run()(implicit sparkSession: SparkSession): Unit = {
    val dataFrameNames: Array[String] = dataFrameName.split(",")
    joinDataFrame(sparkSession, dataFrameNames, joinType, condition, outputDataFrameColumns, outputDataFrameName)
  }


  def joinDataFrame(sparkSession: SparkSession,
                    dataFrameNames: Array[String],
                    joinType: String,
                    condition: String,
                    outputDataFrameColumns: String,
                    outputDataFrameName: String
                   ): Unit = {
    val tempDataFrame0 = sparkSession.table(dataFrameNames(0))
    val tempDataFrame1 = sparkSession.table(dataFrameNames(1))

    if (outputDataFrameColumns.nonEmpty) {
      val tempSelectColumns: Array[Column] = getTwoDataFramesColumn(outputDataFrameColumns, tempDataFrame0, tempDataFrame1)
      val newDF: DataFrame = tempDataFrame0.join(tempDataFrame1, functions.expr(condition), joinType).select(tempSelectColumns: _*)
      newDF.createOrReplaceTempView(outputDataFrameName)
    } else {
      val newDF: DataFrame = tempDataFrame0.join(tempDataFrame1, functions.expr(condition), joinType)
      newDF.createOrReplaceTempView(outputDataFrameColumns)
    }
  }

  def getTwoDataFramesColumn(outputDataFrameColumns: String, tempDataFrame0: DataFrame, tempDataFrame1: DataFrame): Array[Column] = {
    val tempDataFrame0Columns: Array[String] = outputDataFrameColumns.split(";")(0).split(",")
    val tempDataFrame1Columns: Array[String] = outputDataFrameColumns.split(";")(1).split(",")

    val tempDataFrame0ColumnArray = new Array[Column](tempDataFrame0Columns.length)
    for (i <- tempDataFrame0Columns.indices) {
      tempDataFrame0ColumnArray(i) = tempDataFrame0(tempDataFrame0Columns(i))
    }

    val tempDataFrame1ColumnArray = new Array[Column](tempDataFrame1Columns.length)
    for (i <- tempDataFrame0Columns.indices) {
      tempDataFrame1ColumnArray(i) = tempDataFrame0(tempDataFrame1Columns(i))
    }

    val tempSelectColumns: Array[Column] = tempDataFrame0ColumnArray ++ tempDataFrame1ColumnArray
    tempSelectColumns
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
