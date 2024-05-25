package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.core.{Misc, Reader}
import com.payapl.csdmp.sp.factory.InOutputFactory.getReader
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.SparkSession

object MiscOperatorProcessor extends OperatorProcessorTrait with Logging {

  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo("Building misc step: " + step.name)
    val reader: Reader = getReader(step.inputType.get.asInstanceOf[Map[String, Any]], step.name)
  }

  private def processMisc(miscType: Map[String, Any], stepName: String)(implicit sparkSession: SparkSession): Unit = {
    miscType.foreach {
      case (name, params) =>
        val processor: Misc = SpOperatorFactory.getMisc(name, buildParams(stepName, params))
        processor.process(null)
    }
  }
}
