package com.payapl.csdmp.sp.core.output.kafka

import com.payapl.csdmp.sp.core.Writer
import com.payapl.csdmp.sp.logging.KafkaHelper
import com.paypal.csdmp.sp.consts.Constant
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class KafkaWriter(name: String,
                       dataFrameName: String,
                       servers: Option[String],
                       krs: Option[String],
                       topics: String,
                       clientId: String,
                       props: Option[Map[String, String]]
                      ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {
    logInfo("DataFrame should has key and value columns when send to kafka")
    if (krs.isDefined) {
      import scala.collection.JavaConverters._
      val krsConfig: mutable.Map[String, String] = KafkaHelper.getKrsProducerConfig(krs.get, topics, clientId, props.getOrElse(Map.empty)).asScala.map {
        case (k, v) =>
          (if (k.startsWith("kafka")) {
            k
          } else {
            s"kafka.$k"
          }) -> v.toString
      }
      dataFrame
        .selectExpr("cast(key as string", "value")
        .write
        .format(Constant.DATASOURCE_FORMAT_KAFKA)
        .option("topic", topics)
        .options(krsConfig)
        .save()
    } else {
      throw new IllegalArgumentException("non-krs kafka client is not supported")
    }
  }
}
