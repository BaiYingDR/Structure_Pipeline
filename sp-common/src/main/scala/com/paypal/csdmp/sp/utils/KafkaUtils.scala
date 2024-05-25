package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try};

object KafkaUtils extends Logging {

  def getKrsProducer(krsHost: String,
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

  def getKrsProducerConfig(krsHost: String,
                           krsPort: String,
                           colo: String,
                           securityZone: String,
                           clientId: String,
                           topic: String,
                           userConfigs: Map[String, Any]): java.util.Map[String, Object] = {
    getKrsConfig(krsHost, krsPort, colo, securityZone, clientId, topic, userConfigs, "PRODUCER")
  }

  def getKrsConsumerConfig(krsHost: String,
                           krsPort: String,
                           colo: String,
                           securityZone: String,
                           clientId: String,
                           topic: String,
                           userConfigs: Map[String, Any]): java.util.Map[String, AnyRef] = {
    getKrsConfig(krsHost, krsPort, colo, securityZone, clientId, topic, userConfigs, "CONSUMER")
  }

  def getKrsConfig(krsHost: String,
                   krsPort: String,
                   colo: String,
                   securityZone: String,
                   clientId: String,
                   topic: String,,
                   userConfig: Map[String, Any],
                   clientRole: String): java.util.Map[String, AnyRef] = {
    val clientEnv = new ClientEnvironment("java", "kafak-client", "2.2.1-paypal.v4.2")
    val kafkaConfigServiceInput = new KafkaConfigServiceInputBuilder()
      .withColo(colo)
      .withClientId(clientId)
      .withClientRole(clientRole)
      .withClientEnvironment(clientEnv)
      .withSecurityZone(securityZone)
      .withTopics(topic)
      .get

    val configClient = new KafkaConfigClient(krsHost, krsPort, 0)
    import scala.collection.JavaConverters._
    configClient.getC
    onfigs(
      kafkaConfigServiceInput, userConfig.asInstanceOf[Map[String, Object]].asJava
    )
  }

  def getConsumer(config: java.util.Map[String, Object]): KafkaConsumer[AnyRef, AnyRef] = new KafkaConsumer(config)

  implicit class RichProducer(producer: KafkaProducer[AnyRef, AnyRef]) {
    def sendSync(topic: String, message: String): Unit = {
      val record = new ProducerRecord[AnyRef, AnyRef](topic, message)
      val future = producer.send(record)
      producer.flush()
      Try(future.get(10, TimeUnit.SECONDS)) match {
        case Success(metadata) =>
          logInfo(s"${record.value()} got produced at topic-$topic,partition -${metadata.partition()},offset -${metadata.offset()}")
        case Failure(exception) =>
          logWarning(s"${record.value()} could not be produced, Error = ${exception.getMessage()}")
      }
    }

  }
}
