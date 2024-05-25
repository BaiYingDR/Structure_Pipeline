package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.core.Validation
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.SparkSession

object ValidationOperatorProcessor extends OperatorProcessorTrait with Logging {

  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Processing Validation step: ${step.name}")
    processValidation(step.validationType.get.asInstanceOf[Map[String, Any]], step.name)

  }

  def processValidation(validationType: Map[String, Any], stepName: String)(implicit sparkSession: SparkSession): Unit = {
    validationType.foreach {
      case (name, params) =>
        val validator: Validation = SpOperatorFactory.getValidation(name, buildParams(stepName, params))
        validator.validate()


    }
  }

}
