package com.payapl.csdmp.sp.core.misc.offset.sotre

import com.payapl.csdmp.sp.config.SpContext
import com.paypal.csdmp.sp.common.storage.GcsStorageOperator
import org.apache.spark.sql.SparkSession

case class GCSStore()(implicit sparkSession: SparkSession) extends OffsetStore with GcsStorageOperator {

  logInfo("file store loading")
  override val baseDir: String = s"gs://${SpContext.getBucketName}/tmp"
  override implicit val spark: SparkSession = sparkSession
}
