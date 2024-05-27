package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader extends Logging {
  val name: String

  val outputDataFrame: Option[String]

  def read()(implicit sparkSession: SparkSession):DataFrame

}
