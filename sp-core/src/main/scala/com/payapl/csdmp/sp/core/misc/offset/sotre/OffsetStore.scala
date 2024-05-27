package com.payapl.csdmp.sp.core.misc.offset.sotre

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import java.time.LocalDateTime
import java.sql.Timestamp

trait OffsetStore extends Logging {

  val readOpt: String => Option[String]

  val writeContent: (String, String) => Unit

  val baseDir: String

  implicit val spark: SparkSession

  def getOffset(jobId: String, sourceId: String, consumerId: String, batchId: String): Option[Map[String, String]] = {
    logInfo(s"reading offset from file ${buildOffsetFilePath(jobId, sourceId, consumerId)}")
    val dataContent: Option[String] = readOpt(buildOffsetFilePath(jobId, sourceId, consumerId))
    val offsets = dataContent.map(_.split('\n').filter(_.trim.nonEmpty).map(OffsetEntity.unapply(_)).toList)
    getLastOffsets(offsets, batchId)
  }

  def setOffset(jobId: String, sourceId: String, consumerId: String, batchId: String, offset: String) = {

    val offsetFilePath: String = buildOffsetFilePath(jobId, sourceId, consumerId)
    logInfo(s"save offset $offset with ${batchId} to file $offsetFilePath")
    val entity: OffsetEntity = OffsetEntity(sourceId, consumerId, batchId, offsetFilePath, LocalDateTime.now())
    val contents: String = readOpt(offsetFilePath).map(content => addLineBreakToEndIfNotHava(content) + s"${entity.toString}\n").getOrElse(s"${entity.toString}\n")
    writeContent(offsetFilePath, contents)
  }

  /**
   * backend Compatible
   *
   * @param content
   * @return
   */
  def addLineBreakToEndIfNotHava(content: String): String = if (!content.endsWith("\n")) s"$content \n" else content

  def buildOffsetFilePath(jobId: String, sourceId: String, consumerId: String): String = s"$baseDir/${jobId}_${sourceId}_${consumerId}.data"

  def getLastOffsets(data: Option[List[OffsetEntity]], batchId: String)(implicit sparkSession: SparkSession): Option[Map[String, String]] = {
    import sparkSession.implicits._
    data.map(d => {
      val dataframe: DataFrame = d.map(o => Offset(o.batchId, o.offset, Timestamp.valueOf(o.updateTime))).toDF
      val schema: StructType = sparkSession.read.json(dataframe.select("offset").as(Encoders.STRING)).schema
      val res: DataFrame = dataframe.withColumn("json", from_json(col("offset"), schema)).select("json.*", "batchId", "updateTime")
      res.createOrReplaceTempView("tmp")
      schema.fields
        .map(_.name)
        .map(colName => {
          colName -> {
            val res: DataFrame = sparkSession.sql(s"select $colName from tmp where batchId < $batchId and $colName is not null order by batchId desc,updateTime desc limit 1")
            res.collect().headOption.map(_.getString(0))
          }
        })
        .filter { case (_, v) => v.isDefined }
        .map { case (k, v) => k -> v.get }
        .toMap
    })
  }
}
