package com.payapl.csdmp.sp.core.misc.offset.sotre

import com.payapl.csdmp.sp.config.SpContext
import com.paypal.csdmp.sp.common.storage.HdfsStorageOperator
import com.paypal.csdmp.sp.utils.HdfsUtils
import org.apache.spark.sql.SparkSession

case class HDFSStore()(implicit sparkSession: SparkSession) extends OffsetStore with HdfsStorageOperator {

  logInfo("hdfs store loading")

  override val baseDir: String = s"${SpContext.getOffsetHdfsBaseDir}/offset"
  HdfsUtils.mkdirIfNotExist(baseDir)

  override implicit val spark: SparkSession = sparkSession
}
