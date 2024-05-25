package com.paypal.csdmp.sp.builder

import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.SparkSession


trait OperatorProcessorTrait {

  def process(step: Step)(implicit sparkSession: SparkSession): Unit

  def cacheChecker(dfName: Option[String])(implicit sparkSession: SparkSession): Boolean = {
    val cacheDfNameList: Option[String] = sparkSession.conf.getOption("cacheDfNameist")
    (dfName, cacheDfNameList) match {
      case (Some(name), Some(cacheList)) if cacheList.split(",").map(_.trim).contains(name) => true
      case _ => false

    }
  }

  def buildParams(name: String, params: Any): Map[String, Any] = params.asInstanceOf[Map[String, Any]] + ("name" -> name)
}
