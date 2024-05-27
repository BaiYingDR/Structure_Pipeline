package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

trait Validation extends Logging{
  val name: String

  val actualDataFrame: String

  def validate()(implicit sparkSession: SparkSession): Unit
}
