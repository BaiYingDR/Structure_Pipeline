package com.paypal.csdmp.sp.builder

import com.paypal.csdmp.sp.builder.impl.{ExternalCallOperatorProcessor, InputOperatorProcessor, MiscOperatorProcessor, OutputOperatorProcessor, TransformOperatorProcessor, ValidationOperatorProcessor}
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.SparkSession

object StepBuilder extends Logging {

  def build(step: Step)(implicit sparkSession: SparkSession): Unit = {
    if (step.inputType.isDefined) {
      InputOperatorProcessor.process(step)
    } else if (step.outPutType.isDefined) {
      OutputOperatorProcessor.process(step)
    } else if (step.transformType.isDefined) {
      TransformOperatorProcessor.process(step)
    } else if (step.miscType.isDefined) {
      MiscOperatorProcessor.process(step)
    } else if (step.validationType.isDefined) {
      ValidationOperatorProcessor.process(step)
    } else if (step.externalCallType.isDefined) {
      ExternalCallOperatorProcessor.process(step)
    }
  }

}
