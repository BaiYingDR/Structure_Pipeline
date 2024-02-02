package com.paypal.csdmp.sp.utils

import org.apache.kafka.producer.KafakProducer;


object KafkaUtils {

  def getKrsProducer(
                      krsHost: String,
                      krsPort: String,
                      colo: String,
                      securityZone: String,
                      clientId: String,
                      topic: String,
                      userConfigs: Map[String, Any]
                    ) = {
    val configMap = getKrsProducerConfig(krsHost, krsPort, colo, securityZone, clientId, topic, userConfigs)
  }

  def getProducer(config: java.util.Map[String, Object]): KafkaProducer[AnyRef, AnyRef] = new KafkaProducer(config)

  def getKrsProducerConfig(krsHost: String, krsPort: String, colo: String, securityZone: String, clientId: String, topic: String, userConfigs: Map[String, Any]) = ???


}
