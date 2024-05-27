package com.payapl.csdmp.sp.config

import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.utils._

object SpContext {

  private var context: SpContext = _

  def apply(config: ConfigUtils, appArgs: SpSubmitArguments): SpContext = {
    this.context = new SpContext(config, appArgs)
    context
  }

  def getEnv: String = context.env

  def getAppName: String = context.appName

  def getYmlFile: String = context.ymlFile

  def getExternalConfigFileUrl: Option[String] = context.externalConfigFileUrl

  def getVariables: Option[Map[String, String]] = context.variables

  def getBatchId: String = context.batchId

  def getBucketName: String = context.bucketName

  def getSparkMaster: String = context.sparkMaster

  def getOffsetHdfsBaseDir: String = context.offsetHdfsBaseDir

  def getKeyMakerUrl: String = context.keyMakerUrl

  def getEventTracking: Boolean = context.eventTracking

  def getSpApplicationContext: String = context.getSecurityToken("keyMaker.app.context")

  def getDbConfig(sourceId: String) = context.getDBConfig(sourceId)

  def getKrsConfig(krs: String): KrsKafkaInstance = context.getKrsConfig(krs)

  def getHiveUDFs: Seq[Map[String, String]] = context.hiveUDFs

  def getCredentialFromKeyMaker(key: String): String = context.getCredentialFromKeyMaker(key)

  def getOptionMapWithCredentialResolved(key: String): Map[String, String] = context.getOptionMapWithCredentialResolved(key).getOrElse(Map.empty)


}


class SpContext(config: ConfigUtils, appArgs: SpSubmitArguments) extends Logging {
  logInfo("env init...")

  lazy val env: String = config.getEnv

  lazy val appName: String = appArgs.appName

  lazy val ymlFile: String = appArgs.ymlFile

  lazy val externalConfigFileUrl: Option[String] = appArgs.externalConfigFileUrl

  lazy val variables = mergeOptionalMap(appArgs.variables, config.getOptionalMap("sp.conf.variables").map(_.map { case (k, v) => s"@$k" -> v }))

  lazy val batchId = variables.flatMap(_.get("@batchID")).getOrElse(throw new IllegalArgumentException("@batch_id should be provided in command line"))

  lazy val bucketName = variables.flatMap(_.get("@bucketName")).getOrElse(throw new IllegalArgumentException("@bucketName should be provided in command line"))

  lazy val sparkMaster = config.getString("sp.spark.master")

  lazy val keyMakerUrl = config.getString("sp.keymaker.url")

  lazy val offsetHdfsBaseDir = config.getOptionalString("sp.offset.hdfs.basedir").getOrElse("")

  lazy val dbSources = config.getOptionalList[DbInstance]("sp.jdbc.sources").getOrElse(List.empty).map(db => db.alias -> db).toMap

  lazy val krsSources = config.getOptionalList[KrsKafkaInstance]("sp.krs.sources").getOrElse(List.empty).map(krs => krs.alias -> krs).toMap

  lazy val eventTracking = config.getOptionalString("sp.logging.events").map(_.toBoolean).getOrElse(false)

  lazy val hiveUDFs = config.getOptionalList[Map[String, String]]("sp.hive.udf").getOrElse(List.empty)

  private lazy val securityMap = castPropsStringToMap(readContentFromFile(config.getString("sp.security.file.url")))

  def setPayPalProxyIfSet = {
    val paypalProxyInfo: Map[String, String] = config.getOptionalMap("sp.http.proxy").getOrElse(Map.empty)
    val address: Option[String] = paypalProxyInfo.get("address")
    val port: Option[String] = paypalProxyInfo.get("port")
    if (address.isDefined && port.isDefined) {
      HttpUtils.setPaypalProxy(address.get, port.get)
    }
  }

  def getDBConfig(sourceId: String): DbInstance = dbSources.get(sourceId).map(o =>
    o.copy(username = getCredentialFromConfig(o.username), password = getCredentialFromConfig(o.password))
  ).getOrElse(throw new RuntimeException(s"$sourceId doesn't exit in configuration"))

  def getKrsConfig(krs: String): KrsKafkaInstance = krsSources.getOrElse(krs, throw new IllegalArgumentException(s"$krs doesn't exist in krs configuration"))

  def getSecurityToken(key: String): String = securityMap.getOrElse(key, throw new NoSuchElementException(s"value of $key doesn't exist in security file ${config.getString("sp.security.file.url")}"))
1
  def getOptionMapWithCredentialResolved(confKey: String): Option[Map[String, String]] = {
    config.getOptionalMap(confKey).map(_.map { case (key, value) => (key, getCredentialFromConfig(value)) })
  }

  def getCredentialFromConfig(name: String): String = {
    if (name.startsWith("km:") || name.startsWith("km#")) getCredentialFromKeyMaker(name) else name
  }

  def getCredentialFromKeyMaker(key: String): String = KeyMakerUtils.getKeyFromKeyMaker(key, keyMakerUrl, getSecurityToken("keymaker.app.context"))
}


case class DbInstance(alias: String, url: String, username: String, password: String, driver: String)


case class KrsKafkaInstance(alias: String, host: String, port: String, colo: String, securityZone: String)



