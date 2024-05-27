package com.payapl.csdmp.sp.logging

import com.payapl.csdmp.sp.config.{KrsKafkaInstance, SpContext}
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.utils.KafkaUtils
import org.apache.kafka.clients.producer.KafkaProducer

import java.util

object KafkaHelper extends Logging {

  def getProducerAndTopics(producer: String, props: Map[String, String] = Map.empty): (KafkaProducer[AnyRef, AnyRef], String) = {
    val kafkaConfig: Map[String, String] = SpContext.getOptionMapWithCredentialResolved(producer)
    val clientId: Option[String] = kafkaConfig.get("clientId")
    val topic: Option[String] = kafkaConfig.get("topic")
    require(topic.isDefined, "topic should be defined for kafka")
    require(clientId.isDefined, "clientId should be defined for kafka")
    val krsOpt: Option[String] = kafkaConfig.get("krs")
    if (krsOpt.isDefined) {
      getKrsProducerAndTopics(krsOpt.get, topic.get, clientId.get, props)
    } else {
      getNonKrsProducerAndTopics
    }
  }


  def getKrsProducerAndTopics(krs: String,
                              topics: String,
                              clientId: String,
                              props: Map[String, String] = Map.empty): (KafkaProducer[AnyRef, AnyRef], String) = {
    val config: util.Map[String, Object] = getKrsProducerConfig(krs, topics, clientId, props)
    val producer: KafkaProducer[AnyRef, AnyRef] = KafkaUtils.getProducer(config)
    (producer, topics)
  }


  def getKrsProducerConfig(krs: String,
                           topics: String,
                           clientId: String,
                           props: Map[String, String] = Map.empty): util.Map[String, Object] = {
    val krsInstance: KrsKafkaInstance = SpContext.getKrsConfig(krs)
    logInfo(s"kafka producer with ${krsInstance.host}:${krsInstance.port} initialized for event tracking")

    val userConfigs: Map[String, Any] = Map(
      "paypal.nonraptor.client" -> true,
      "ssl.paypal.client.appcontext" -> SpContext.getSpApplicationContext,
      "client.id" -> clientId
    ) ++ props

    KafkaUtils.getKrsProducerConfig(
      krsInstance.host,
      krsInstance.port,
      krsInstance.colo,
      krsInstance.securityZone,
      clientId,
      topics,
      userConfigs
    )
  }

  def getKrsConsumerConfig(krs: String,
                           topics: String,
                           clientId: String,
                           props: Map[String, String] = Map.empty): java.util.Map[String, AnyRef] = {
    val krsInstance: KrsKafkaInstance = SpContext.getKrsConfig(krs)
    logInfo(s"kafka producer with ${krsInstance}:${krsInstance.port} initialized for event tracking")
    val userConfigs: Map[String, Any] = Map("paypal.nonraptor.client" -> true,
      "ssl.paypal.client.appcontext" -> SpContext.getSpApplicationContext,
      "client.id" -> clientId
    ) ++ props

    KafkaUtils.getKrsConsumerConfig(
      krsInstance.host,
      krsInstance.port,
      krsInstance.colo,
      krsInstance.securityZone,
      clientId,
      topics,
      userConfigs
    )
  }

  def getNonKrsProducerAndTopics: (KafkaProducer[AnyRef, AnyRef], String) = {
    throw new IllegalArgumentException(s"non-krs kafka producer is not support currently")
  }
}
