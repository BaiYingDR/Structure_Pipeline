package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.core.Transformer
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformOperatorProcessor extends OperatorProcessorTrait with Logging {


  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Building transform step: ${step.name}")
    val transformer: Transformer = getTransformer(step.transformType.get.asInstanceOf[Map[String, Any]], step.name)

    logInfo(s"Calculating step ${step.name}")
    transformer.run()

    val dfName: Option[String] = transformer.getOutputDfName()
    if (cacheChecker(dfName)) {
      val df: DataFrame = sparkSession.table(dfName.get)
      df.cache()
      val rowCount: Long = df.count()
      logInfo(s"the transform dataframe ${dfName.get} has been cached with total $rowCount rows")
    }
  }

  def getTransformer(transformationType: Map[String, Any], stepName: String = "temp"): Transformer = {
    transformationType.map { case (name, params) => SpOperatorFactory.getTransformer(name, buildParams(stepName, params))}.headOption
      .getOrElse(throw new RuntimeException("transformer is not existed"))
  }
}
