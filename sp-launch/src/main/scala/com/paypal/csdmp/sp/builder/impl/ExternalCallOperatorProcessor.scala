package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.core.ExternalCall
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.SparkSession

object ExternalCallOperatorProcessor extends OperatorProcessorTrait with Logging {


  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Processing ExternalCall Step: ${step.name}")
    processExternalCall(step.externalCallType.get.asInstanceOf[Map[String, Any]], step.name)
  }

  def processExternalCall(externalCallType: Map[String, Any], stepName: String)(implicit sparkSession: SparkSession): Unit = {
    externalCallType.foreach { case (name, params) =>
      val processor: ExternalCall = SpOperatorFactory.getExternalCallType(name, buildParams(stepName, params))
      processor.process()
    }
  }
}
