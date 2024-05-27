package com.payapl.csdmp.sp.core.misc.offset.sotre

import com.paypal.csdmp.sp.common.storage.FileStorageOperator
import org.apache.spark.sql.SparkSession

case class FileStore()(implicit sparkSession: SparkSession) extends OffsetStore with FileStorageOperator {

  logInfo("file store loading")
  override val baseDir: String = "/tmp"
  override implicit val spark: SparkSession = sparkSession
}
