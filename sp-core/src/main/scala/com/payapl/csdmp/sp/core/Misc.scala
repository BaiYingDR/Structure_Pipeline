package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

trait Misc extends Logging {

  val name: String

  def process(keys: List[String])(implicit sparkSession: SparkSession): Unit

}
