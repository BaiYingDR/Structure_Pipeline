package com.payapl.csdmp.sp.core

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.DataFrame

trait Writer extends Serializable with Logging {

  val name: String

  val dataFrameName: String

  def write(dataFrame: DataFrame): Unit

}
