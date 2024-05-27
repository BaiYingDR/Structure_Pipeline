package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import com.paypal.csdmp.sp.exception.InvalidArgumentException
import com.paypal.csdmp.sp.utils.SparkFunctionUtils.{SORT_FUNCTION_MAP, addSorts, getWindow}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

case class DuplicateTransformer(name: String,
                                dataFrameName: String,
                                partitionCols: String,
                                sortCols: Map[String, String],
                                outputDataFrameName: String
                               ) extends Transformer {


  override def run()(implicit sparkSession: SparkSession): Unit = {

    val tempDataFrame0: DataFrame = sparkSession.table(dataFrameName)
    removeDuplicate(tempDataFrame0, partitionCols, sortCols, outputDataFrameName)
  }

  def removeDuplicate(dataFrame: DataFrame, partitionCols: String, sortCols: Map[String, String], outputDataFrameName: String): Unit = {
    var df: DataFrame = dataFrame
    val partitionColumns: Some[List[String]] = Some(partitionCols.split(",").map(i => i.mkString).toList)

    val sorts: Option[List[Column]] = Option(sortCols.map(col => {
      val sortType: String = col._2
      if (SORT_FUNCTION_MAP.contains(sortType)) {
        SORT_FUNCTION_MAP(sortType)(col._1)
      } else {
        throw new InvalidArgumentException(s"Invalid Sort Type: ${sortType},please enter invalid sort Type")
      }
    }).toList)

    var window: WindowSpec = getWindow(partitionColumns, None)
    window = addSorts(window, sorts)
    df = df.withColumn("Rank", functions.row_number().over(window))
    df = df.where("Rank == 1").drop("Rank")
    df.createOrReplaceTempView(outputDataFrameName)
  }

  override def getOutputDfName(): Option[String] = Some(outputDataFrameName)
}
