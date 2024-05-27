package com.payapl.csdmp.sp.core.validation

import com.payapl.csdmp.sp.core.Validation
import com.paypal.csdmp.sp.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DataframeValidation(name: String, actualDataFrame: String, exceptedDataframe: String) extends Validation {

  override def validate()(implicit sparkSession: SparkSession): Unit = {
    val actualDF: DataFrame = sparkSession.table(actualDataFrame)
    val expectedDF: DataFrame = sparkSession.table(actualDataFrame)
    DataFrameUtils.asserEquals(actualDF, expectedDF)
  }
}
