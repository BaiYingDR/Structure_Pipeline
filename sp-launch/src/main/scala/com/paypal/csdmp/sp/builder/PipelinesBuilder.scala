package com.paypal.csdmp.sp.builder

import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.pojo.{Configuration, Pipeline}
import org.apache.spark.sql.SparkSession

object PipelinesBuilder extends Logging {

  def build(config: Configuration)(implicit sparkSession: SparkSession): Unit = {
    logInfo("Building Pipeline")

    config.pipelines.getOrElse(Nil).foreach(
      p => {
        val pipeline: Pipeline = p("pipeline")
        if (pipeline.enable.getOrElse(true)) {
          PipelineBuilder.build(pipeline)
        }
      }
    )
  }

}
