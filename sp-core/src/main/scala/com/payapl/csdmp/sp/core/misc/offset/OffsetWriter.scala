package com.payapl.csdmp.sp.core.misc.offset

import com.payapl.csdmp.sp.config.SpContext
import com.payapl.csdmp.sp.core.Misc
import com.payapl.csdmp.sp.core.misc.offset.StoreMgt.getStore
import com.payapl.csdmp.sp.core.misc.offset.sotre.OffsetStore
import com.paypal.csdmp.sp.utils.JsonUtils._
import com.paypal.csdmp.sp.utils.mergeOptionalMap
import org.apache.spark.sql.SparkSession

case class OffsetWriter(name: String,
                        jobId: String,
                        sourceId: String,
                        consumerId: String,
                        sql: Option[String],
                        sparkProps: Option[String],
                        store: String = "gcs"
                       ) extends Misc {


  override def process(keys: List[String])(implicit sparkSession: SparkSession): Unit = {
    val values: Option[Map[String, String]] = mergeOptionalMap(getOffsetFromDf(sql), getOffsetFromSparkProps(sparkProps))
    values.foreach(v => {
      v.foreach {
        case (k, v) => if (v != null) sparkSession.conf.set(k, v)
      }
      saveOffset(v.toJson)
    })
  }

  def saveOffset(offsets: String)(implicit sparkSession: SparkSession): Unit = {
    val storeImpl: OffsetStore = getStore(store)
    storeImpl.setOffset(jobId, sourceId, consumerId, SpContext.getBatchId, offsets)
  }

  def getOffsetFromDf(sql: Option[String])(implicit sparkSession: SparkSession): Option[Map[String, String]] = {
    for (s <- sql; df = sparkSession.sql(s); head <- df.collect().headOption) yield {
      df.columns.map(col => col -> head.getAs[String](col)).toMap
    }
  }

  def getOffsetFromSparkProps(sparkProps: Option[String])(implicit sparkSession: SparkSession): Option[Map[String, String]] = {
    sparkProps.map(
      _.split(",")
        .map(_.trim)
        .map(k => k -> sparkSession.conf.get(k, null))
        .toMap
    )
  }
}
