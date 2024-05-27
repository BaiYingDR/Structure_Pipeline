package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

trait ExternalCall extends Logging {

  val name: String

  def process()(implicit sparkSession: SparkSession): Unit

}
