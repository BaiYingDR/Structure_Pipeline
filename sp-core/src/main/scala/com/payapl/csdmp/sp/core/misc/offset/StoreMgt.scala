package com.payapl.csdmp.sp.core.misc.offset

import com.payapl.csdmp.sp.core.misc.offset
import com.payapl.csdmp.sp.core.misc.offset.StoreType.StoreType
import com.payapl.csdmp.sp.core.misc.offset.sotre.{FileStore, GCSStore, HDFSStore, OffsetStore}
import org.apache.spark.sql.SparkSession

object StoreMgt {
  val DEFAULT_STORE: StoreType.Value = StoreType.FILE

  def getStore(store: String)(implicit sparkSession: SparkSession): OffsetStore = getStore(StoreType.withName(store))

  def getStore(store: StoreType)(implicit sparkSession: SparkSession): OffsetStore = store match {
    case StoreType.FILE => FileStore()
    case StoreType.GCS => GCSStore()
    case StoreType.HDFS => HDFSStore()
  }
}

object StoreType extends Enumeration {
  type StoreType = Value
  val FILE: offset.StoreType.Value = Value(1, "file")
  val GCS: offset.StoreType.Value = Value(1, "GCS")
  val HDFS: offset.StoreType.Value = Value(1, "HDFS")
}
