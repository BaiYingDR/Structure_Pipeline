package com.payapl.csdmp.sp.core.misc

import com.payapl.csdmp.sp.core.Misc
import org.apache.spark.sql.SparkSession

case class CacheHelper(name: String,
                       dfNameList: String
                      ) extends Misc {
  override def process(keys: List[String])(implicit sparkSession: SparkSession): Unit = {
    sparkSession.conf.set("cacheDfNameList", dfNameList)
    logInfo(s"cacheDfNameList: $dfNameList")
  }

}
