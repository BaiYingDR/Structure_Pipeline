package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import org.apache.spark.sql.SparkSession

case class DefaultValueTransformer(name: String,
                                   dataFrameName: String,
                                   columnAndValue: Map[String, Object]
                                  ) extends Transformer {

  override def run()(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Start to fill default value with user's configuration")
    logInfo(s"columnAndValue is ${columnAndValue.mkString}")
    sparkSession.table(dataFrameName).na.fill(columnAndValue).createOrReplaceTempView(dataFrameName)

  }

  override def getOutputDfName(): Option[String] = None
}
