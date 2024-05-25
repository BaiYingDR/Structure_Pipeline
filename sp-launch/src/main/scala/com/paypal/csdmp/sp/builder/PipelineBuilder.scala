package com.paypal.csdmp.sp.builder

import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.pojo.{Pipeline, Step}
import org.apache.spark.sql.SparkSession

object PipelineBuilder extends Logging {

  def build(pipeline: Pipeline)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Building pipeline: ${pipeline.name}")
    val initialStepsList: List[String] = pipeline.initialSteps.getOrElse(Nil)
    val finalStepsList: List[String] = pipeline.finalSteps.getOrElse(Nil)
    val stepList: List[Map[String, Step]] = pipeline.steps.getOrElse(Nil)

    try {
      //process only initials
      stepList
        .filter(step => initialStepsList.contains(step("step").name))
        .foreach(step => {
          logInfo(s"Processing Initial step: ${step("step").name}")
          StepBuilder.build(step("step"))
        })

      //process others but not initial @ final steps.
      stepList
        .filter(step => !initialStepsList.contains(step("step").name) && !finalStepsList.contains(step("step").name))
        .foreach(step => {
          logInfo(s"Processing intermediate step: ${step("step").name}")
          StepBuilder.build(step("step"))
        })
    } catch {
      case e: Throwable => {
        logError(s"Pipeline Job failed, Exception in PipelineBuilder, ${e.getMessage}")
        sparkSession.conf.set("app_error_message", "Err: " + e.getMessage)
        throw e
      }
    } finally {
      //process in finally blocks
      stepList
        .filter(step => finalStepsList.contains(step("name").name))
        .foreach(step => {
          logInfo(s"Processing Final step: ${step("name").name}")
          StepBuilder.build(step("step"))
        })
    }
  }
}
