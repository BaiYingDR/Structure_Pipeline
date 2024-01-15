package com.paypal.csdmp.sp.consts

object Constant {

  /**
   * SCHEMA PREFIX
   */
  val GCS_PATH_PREFIX = "gs://"
  val HDFS_PATH_PREFIX = "hdfs://"
  val RESOURCE_PATH_PREFIX = "resource://"

  /**
   * DATASOURCE
   */
  val DATASOURCE_FORMAT_HIVE = "hive"
  val DATASOURCE_FORMAT_BIGQUERY = "org.apache.spark.sql.execution.datasources.bigquery2"
  val DATASOURCE_FORMAT_KAFAK = "kafka"

  /**
   * SYMBOL
   */

  val SYMBOL_COMMA = ","
  val SYMBOL_SLASH = "/"

}
