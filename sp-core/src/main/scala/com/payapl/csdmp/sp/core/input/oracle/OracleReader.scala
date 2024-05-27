package com.payapl.csdmp.sp.core.input.oracle

import com.payapl.csdmp.sp.core.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class OracleReader extends Reader() {

  override val name: String = _

  override val outputDataFrame: Option[String] = _

  override def read()(implicit sparkSession: SparkSession): DataFrame = ???
}
