package com.payapl.csdmp.sp.core.input.kafka

import com.payapl.csdmp.sp.core.Reader
import com.payapl.csdmp.sp.logging.KafkaHelper
import com.paypal.csdmp.sp.consts.Constant
import com.paypal.csdmp.sp.utils.JsonUtils.Json2Obj
import com.paypal.csdmp.sp.utils.{JsonUtils, KafkaUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

case class KafkaReader(name: String,
                       outputDataFrame: Option[String],
                       krs: Option[String],
                       server: Option[String],
                       topics: String,
                       clientId: String = "",
                       props: Option[Map[String, String]],
                       lastOffset: String = "earliest",
                       setNextExceptedOffset: Boolean = false
                      ) extends Reader {


  override def read()(implicit sparkSession: SparkSession): DataFrame = {
    logInfo("kafka reader is starting")
    if (krs.isDefined) {
      import scala.collection.JavaConverters._
      val krsConfig: mutable.Map[String, String] = KafkaHelper.getKrsConsumerConfig(krs.get, topics, clientId, props.getOrElse(Map.empty))
        .asScala
        .map { case (k, v) => (if (k.startsWith("kafka")) k else s"kafka.$k") -> v.toString }
      logInfo(s"kafka server:$server,Kafka topics:$topics")
      logInfo(s"offset is $lastOffset")
      val df: DataFrame = sparkSession
        .read
        .format(Constant.DATASOURCE_FORMAT_KAFKA)
        .option("subscribe", topics)
        .option("startingOffsets", lastOffset)
        .options(krsConfig)
        .load()

      if (setNextExceptedOffset) setOffset(df)
      df.selectExpr("cast(key as string)", "cast(value as string)", "topic", "partition", "offset", "timestamp", "timestampType")
    } else {
      throw new IllegalArgumentException(s"non-krs kafka client is not supported")
    }
  }

  private def setOffset(df: DataFrame) = {
    val last =
      try {
        lastOffset.as[Map[String, Map[String, Number]]].flatMap { case (topic, po) =>
          po.map { case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.longValue() }
        }
      } catch {
        case _: Exception => Map.empty
      }

    val offset: DataFrame = df.selectExpr("topic", "partition", "offset")
    offset.createOrReplaceTempView("k_metadata")
    val latestOffset: DataFrame = df.sparkSession.sql("select topic,partition,max(offset) as offset from k_metadata group by topic partition")
    val latest: Map[TopicPartition, Long] = latestOffset.collect().map(r => new TopicPartition(r.getString(0), r.getInt(1)) -> (r.getLong(2) + 1)).toMap
    val userConfigs = Map(
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val consumerConfig: java.util.Map[String, AnyRef] = KafkaHelper.getKrsConsumerConfig(krs.get, topics, clientId, userConfigs)
    val consumer: KafkaConsumer[AnyRef, AnyRef] = KafkaUtils.getConsumer(consumerConfig)

    import scala.collection.JavaConverters._
    val default: Map[TopicPartition, Long] = consumer.partitionsFor(topics).asScala.map(pi => new TopicPartition(pi.topic(), pi.partition()) -> -2L).toMap
    val finalOffset: Map[TopicPartition, Long] = default ++ last ++ latest
    val finalOffsetDetail: Map[String, Map[String, Long]] = finalOffset.groupBy { case (tp, _) => tp.topic() }.map { case (topic, tpo) =>
      topic -> tpo.map { case (tp, offset) => tp.partition().toString -> offset }
    }
    df.sparkSession.conf.set("last_offset", JsonUtils.toJson(finalOffsetDetail))
  }

}




