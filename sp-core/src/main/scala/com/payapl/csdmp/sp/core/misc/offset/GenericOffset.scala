package com.payapl.csdmp.sp.core.misc.offset

import com.payapl.csdmp.sp.config.SpContext
import com.payapl.csdmp.sp.core.misc.offset.StoreMgt.getStore
import com.payapl.csdmp.sp.core.misc.offset.sotre.OffsetStore
import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

case class GenericOffset(jobId: String,
                         sourceId: String,
                         consumerId: String,
                         store: String = "gcs",
                         defaultMap: Map[String, String] = Map.empty) extends Logging {
  def getOffset()(implicit sparkSession: SparkSession): Map[String, String] = {
    val storeImpl: OffsetStore = getStore(StoreType.withName(store))
    val offsets: Option[Map[String, String]] = storeImpl.getOffset(jobId, sourceId, consumerId, SpContext.getBatchId)
    if (offsets.isEmpty) {
      logWarning("could not find any value in this store, use default value for offset")
    }
    offsets.map(defaultMap ++ _).getOrElse(defaultMap).map { case (k, v) => s"@$k" -> v }
  }
}
