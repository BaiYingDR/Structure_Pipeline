package com.paypal.csdmp.sp.launch

import com.payapl.csdmp.sp.config.{ConfigUtils, SpContext, SpSubmitArguments}
import com.payapl.csdmp.sp.core.misc.offset.GenericOffset
import com.payapl.csdmp.sp.listener.SparkMetricsListener
import com.paypal.csdmp.sp.builder.PipelinesBuilder
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.pojo.Configuration
import com.paypal.csdmp.sp.utils.JsonUtils.Json2Obj
import com.paypal.csdmp.sp.utils.ReflectionUtils.beanFromMap
import com.paypal.csdmp.sp.utils.{DateTimeUtils, mergeOptionalMap, readContentFromFile, registerUDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hive2.HiveFunctionRegister

object SpJobApplication extends Logging {

  def main(args: Array[String]): Unit = {
    val appArgs: SpSubmitArguments = SpSubmitArguments(args)
    val config: ConfigUtils = ConfigUtils(appArgs)
    logInfo(s"load the config: ${config.toString}")
    SpContext(config, appArgs)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(SpContext.getAppName)
      .master(SpContext.getSparkMaster)
      .enableHiveSupport()
      .getOrCreate()

    registerBuildInUDF
    registerConfiguredHiveUDF
    setCustomListener()
    auditJobInfoWhileInit()
    setTimeZone()
    execute()
  }

  def execute()(implicit sparkSession: SparkSession) = {
    SpContext.getYmlFile.split(",").zipWithIndex.foreach {
      case (file, idx) =>
        logInfo(s"execute the number ${idx + 1}yml file: $file")
        val content: String = readContentFromFile(file)
        val replaceMap: Option[Map[String, String]] = mergeOptionalMap(SpContext.getVariables, getOffset(injectVarsToYaml(content, SpContext.getVariables)))
        logInfo(s"variable map to replace ${replaceMap}")
        replaceMap.foreach(map => map.foreach { case (k, v) => sparkSession.conf.set(k, v) })
        val yml: String = injectVarsToYaml(content, replaceMap)
        logInfo(s"building pipeline: $yml")
        PipelinesBuilder.build(yml.as[Configuration])
        SparkMetricsListener.getMetricsInfo(sparkSession)
    }
  }

  def getOffset(yaml: String)(implicit sparkSession: SparkSession): Option[Map[String, String]] = {
    yaml.as[Configuration].offset.flatMap(_.generic).map(map => beanFromMap[GenericOffset](map + ("name" -> "offset")).getOffset)
  }

  private def injectVarsToYaml(yaml: String, variables: Option[Map[String, String]]): String = {
    val tmp: String = yaml
    variables.foreach(_ foreach { case (k, v) => tmp.replace(k, v) })
    tmp
  }

  def setTimeZone()(implicit sparkSession: SparkSession): Unit = {
    val timeZoneConf: Option[String] = sparkSession.conf.getOption("spark.session.timeZone")
    timeZoneConf match {
      case Some(timeZone) =>
        logInfo(s"time zone is set, we get it: ${timeZone}")
        sparkSession.conf.set("spark.sql.session.timeZone", timeZone)
      case None =>
        logInfo("time zone is not set, we choose UTC")
        sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    }
  }

  def setCustomListener()(implicit sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.addSparkListener(new SparkMetricsListener)
  }

  def auditJobInfoWhileInit()(implicit sparkSession: SparkSession): Unit = {
    sparkSession.conf.set("@adt_batch_start_date", DateTimeUtils.getCurrentUTCTimestampStr)
    sparkSession.conf.set("@adt_appName", SpContext.getAppName)
    sparkSession.conf.set("@adt_ymlFile", SpContext.getYmlFile)
    sparkSession.conf.set("@adt_env", SpContext.getEnv)
    sparkSession.conf.set("@adt_variablesMap", SpContext.getVariables.getOrElse(Map()).toString())
    sparkSession.conf.set("@adt_externalConfigFileUrl", SpContext.getExternalConfigFileUrl.getOrElse("none"))
  }

  private def registerBuildInUDF()(implicit sparkSession: SparkSession): Unit = {
    try {
      registerUDF("com.paypal.csdmp.sp.ext.udf.BuildInFunctions")
      logWarning("found spark udf build-in functions from class com.paypal.csdmp.sp.ext.udf.BuildInFunctions and registered")
    } catch {
      case _: ClassNotFoundException => logInfo("no build-in spark udf functions detected../")
    }
  }

  private def registerConfiguredHiveUDF()(implicit sparkSession: SparkSession): Unit = {
    SpContext.getHiveUDFs.foreach(udf => {
      (udf.get("alias"), udf.get("class")) match {
        case (Some(alias), Some(clazz)) => {
          HiveFunctionRegister.registerHiveFunction(alias, clazz)
          logInfo(s"register hive udf function $clazz as alias $alias successfully")
        }
        case _ => logWarning(s"failed to register hive udf with configuration $udf")
      }
    })
  }
}
