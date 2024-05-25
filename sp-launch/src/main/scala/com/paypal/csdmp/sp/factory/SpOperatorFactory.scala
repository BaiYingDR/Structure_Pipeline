package com.paypal.csdmp.sp.factory

import com.payapl.csdmp.sp.core.externalCall.bigquery.{BigQueryDeleteCaller, BigQuerySQLCaller, BigQueryUpdateCaller}
import com.payapl.csdmp.sp.core.input.bigquery.BigQueryReader
import com.payapl.csdmp.sp.core.input.file.FileReader
import com.payapl.csdmp.sp.core.input.gcs.GcsReader
import com.payapl.csdmp.sp.core.input.hive.HiveReader
import com.payapl.csdmp.sp.core.input.kafka.KafkaReader
import com.payapl.csdmp.sp.core.input.oracle.OracleReader
import com.payapl.csdmp.sp.core.misc.{CacheHelper, DataChecker, FileCmdOperator}
import com.payapl.csdmp.sp.core.misc.batchAuditor.BatchAuditor
import com.payapl.csdmp.sp.core.output.ShowWriter
import com.payapl.csdmp.sp.core.output.bigquery.{BigQueryMergeWriter, BigQueryWriter}
import com.payapl.csdmp.sp.core.output.csv.CsvWriter
import com.payapl.csdmp.sp.core.output.file.FileWriter
import com.payapl.csdmp.sp.core.output.gcs.GcsWriter
import com.payapl.csdmp.sp.core.output.hdfs.HdfsWriter
import com.payapl.csdmp.sp.core.output.hive.{HiveMergeWriter, HiveWriter}
import com.payapl.csdmp.sp.core.output.kafka.KafkaWriter
import com.payapl.csdmp.sp.core.trasnform.{CheckPointOperator, DefaultValueTransformer, DuplicateTransformer, InferSchemaTransformer, JSONExtractTransformer, JoinTransformer, KafkaPPDeserTransformer, RDSSchemaTransformer, SparkSQLTransformer}
import com.payapl.csdmp.sp.core.validation.{CountValidation, DataframeValidation, ThresholdValidation}
import com.payapl.csdmp.sp.core.{ExternalCall, Misc, Reader, Transformer, Validation, Writer}
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.utils.{ClassLoaderUtils, ReflectionUtils}
import com.paypal.csdmp.sp.utils.ReflectionUtils.beanFromMap

import java.util.ServiceLoader

object SpOperatorFactory extends Logging {

  def getInput(name: String, params: Map[String, Any]): Reader = {
    name
    match {
      case "oracleInput" => beanFromMap[OracleReader](params)
      case "fileInput" => beanFromMap[FileReader](params)
      case "hiveInput" => beanFromMap[HiveReader](params)
      case "bigQueryInput" => beanFromMap[BigQueryReader](params)
      case "gcsInput" => beanFromMap[GcsReader](params)
      case "kafkaInput" => beanFromMap[KafkaReader](params)
      case _ =>
        val loader: ClassLoader = ClassLoaderUtils.getContextOrSparkClassLoader
        val serviceLoader: ServiceLoader[Reader] = ServiceLoader.load(classOf[Reader], loader)
        import scala.collection.JavaConversions._
        serviceLoader.foreach(v => logInfo(s"loading third party reader ${v.getClass.getCanonicalName}."))
        val
        service: Option[Reader] = serviceLoader.find(r => name.equalsIgnoreCase(r.getClass.getSimpleName.replace("Reader", "Input"))).headOption
        service.map(s => ReflectionUtils.beanFromMap(params, s.getClass)).getOrElse(throw new IllegalArgumentException(s"$name input type is not found in local or third party package."))
    }
  }

  def getTransformer(name: String, map: Map[String, Any]): Transformer = {
    name match {
      case "dfJoinTransformer" => beanFromMap[JoinTransformer](params)
      case "dfSparkSQLTransformer" => beanFromMap[SparkSQLTransformer](params)
      case "dfCheckPointOperator" => beanFromMap[CheckPointOperator](params)
      case "dfDuplicateTransformer" => beanFromMap[DuplicateTransformer](params)
      case "dfDefaultValueTransformer" => beanFromMap[DefaultValueTransformer](params)
      case "dfKafkaPPDeserTransformer" => beanFromMap[KafkaPPDeserTransformer](params)
      case "dfRDSSchemaTransformer" => beanFromMap[RDSSchemaTransformer](params)
      case "dfJSONExtractTransformer" => beanFromMap[JSONExtractTransformer](params)
      case "inferSchemaTransformer" => beanFromMap[InferSchemaTransformer](params)
      case _ =>
        val loader: ClassLoader = ClassLoaderUtils.getContextOrSparkClassLoader
        val serviceLoader: ServiceLoader[Transformer] = ServiceLoader.load(classOf[Transformer], loader)
        import scala.collection.JavaConversions._
        serviceLoader.foreach(v => logInfo(s"loading third party reader ${v.getClass.getCanonicalName}..."))
        val
        service: Option[Transformer] = serviceLoader.find(r => name.equalsIgnoreCase(r.getClass.getSimpleName)).headOption
        service.map(s => ReflectionUtils.beanFromMap(params, s.getClass))
          .getOrElse(throw new IllegalArgumentException(s"$name Transformer type is not found in local or third party package."))
    }
  }


  def getOutput(name: String, params: Map[String, Any]): Writer = {
    name match {
      case "hiveOutput" => beanFromMap[HiveWriter](params)
      case "gcsOutput" => beanFromMap[GcsWriter](params)
      case "bigQueryOutput" => beanFromMap[BigQueryWriter](params)
      case "showOutput" => beanFromMap[ShowWriter](params)
      case "hdfsOutput" => beanFromMap[HdfsWriter](params)
      case "csvOutput" => beanFromMap[CsvWriter](params)
      case "hiveMergeOutput" => beanFromMap[HiveMergeWriter](params)
      case "bigQueryMergeOutput" => beanFromMap[BigQueryMergeWriter](params)
      case "kafkaOutput" => beanFromMap[KafkaWriter](params)
      case "fileOutput" => beanFromMap[FileWriter](params)
      case _ =>
        val loader: ClassLoader = ClassLoaderUtils.getContextOrSparkClassLoader
        val serviceLoader: ServiceLoader[Writer] = ServiceLoader.load(classOf[Writer], loader)
        import scala.collection.JavaConversions._
        serviceLoader.foreach(v => logInfo(s"loading third party reader ${v.getClass.getCanonicalName}..."))
        val
        service: Option[Writer] = serviceLoader.find(r => name.equalsIgnoreCase(r.getClass.getSimpleName)).headOption
        service.map(s => ReflectionUtils.beanFromMap(params, s.getClass))
          .getOrElse(throw new IllegalArgumentException(s"$name output type is not found in local or third party package."))
    }
  }

  def getMisc(name: String, map: Map[String, Any]): Misc = {
    name match {
      case "batchAuditor" => beanFromMap[BatchAuditor](params)
      case "offsetSaver" => beanFromMap[OffsetWriter](params)
      case "DataChecker" => beanFromMap[DataChecker](params)
      case "DropZoneUploader" => beanFromMap[DropZoneUploader](params)
      case "DonefileGenerator" => beanFromMap[DonefileGenerator](params)
      case "CacheHelper" => beanFromMap[CacheHelper](params)
      case "dropZoneFetcher" => beanFromMap[DropZoneFetcher](params)
      case "decompressionHelper" => beanFromMap[DecompressionHelper](params)
      case "compressionHelper" => beanFromMap[CompressionHelper](params)
      case "fileCmdOperator" => beanFromMap[FileCmdOperator](params)
      case _ =>
        val loader: ClassLoader = ClassLoaderUtils.getContextOrSparkClassLoader
        val serviceLoader: ServiceLoader[Misc] = ServiceLoader.load(classOf[Misc], loader)
        import scala.collection.JavaConversions._
        serviceLoader.foreach(v => logInfo(s"loading third party reader ${v.getClass.getCanonicalName}..."))
        val
        service: Option[Misc] = serviceLoader.find(r => name.equalsIgnoreCase(r.getClass.getSimpleName)).headOption
        service.map(s => ReflectionUtils.beanFromMap(params, s.getClass))
          .getOrElse(throw new IllegalArgumentException(s"$name misc type is not found in local or third party package."))
    }
  }

  def getValidation(name: String, params: Map[String, Any]): Validation = {
    name match {
      case "dataframeValidation" => beanFromMap[DataframeValidation](params)
      case "countValidation" => beanFromMap[CountValidation](params)
      case "thresholdValidation" => beanFromMap[ThresholdValidation](params)
      case _ =>
        val loader: ClassLoader = ClassLoaderUtils.getContextOrSparkClassLoader
        val serviceLoader: ServiceLoader[Validation] = ServiceLoader.load(classOf[Validation], loader)
        import scala.collection.JavaConversions._
        serviceLoader.foreach(v => logInfo(s"loading third party reader ${v.getClass.getCanonicalName}..."))
        val service: Option[Validation] = serviceLoader.find(r => name.equalsIgnoreCase(r.getClass.getSimpleName)).headOption
        service.map(s => ReflectionUtils.beanFromMap(params, s.getClass))
          .getOrElse(throw new IllegalArgumentException(s"$name validation type is not found in local or third party package."))
    }
  }


  def getExternalCallType(name: String, params: Map[String, Any]): ExternalCall = {
    name match {
      case "bigQueryUpdateCall" => beanFromMap[BigQueryUpdateCaller](params)
      case "bigQueryDeleteCall" => beanFromMap[BigQueryDeleteCaller](params)
      case "bigQuerySQLCall" => beanFromMap[BigQuerySQLCaller](params)
      case _ => throw new IllegalArgumentException(s"$name external call type is not valid")
    }
  }

}
