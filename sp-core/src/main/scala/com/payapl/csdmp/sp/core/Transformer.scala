package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

trait Transformer extends Logging {

  val name: String

  def run()(implicit sparkSession: SparkSession): Unit

  def getOutputDfName(): Option[String]

}
