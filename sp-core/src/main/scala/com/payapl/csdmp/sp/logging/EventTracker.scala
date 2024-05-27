package com.payapl.csdmp.sp.logging

import com.payapl.csdmp.sp.config.SpContext
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.utils.JsonUtils.Obj2Json
import com.paypal.csdmp.sp.utils.KafkaUtils.RichProducer

object EventTracker extends Logging {

  private val PERMITTED_LOGGING_ENV = List("prod", "pre", "gray")

  lazy val shouldTracking: Boolean = SpContext.getEventTracking && PERMITTED_LOGGING_ENV.contains(SpContext.getEnv)

  lazy val (producer, topic) = if (shouldTracking) {
    logInfo(s"event tracking initialized for ${SpContext.getEnv} env ...")
    try {
      getProducer
    } catch {
      case e: Exception => {
        logWarning("failed to init kafka producer in EventTracker class", e)
        (None, "")
      }
    }
  } else (None, "")

  private def getProducer = {
    val userConfigs = Map(
      "max.block.ms" -> "60000",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
    val (producer, topics) = KafkaHelper.getProducerAndTopics("sp.kafka.producer.logging", userConfigs)
    (Some(producer), topics)
  }

  def logging(obj: => AnyRef): Unit = {
    producer.foreach(_.sendSync(topic, obj.toJson))
  }

}
