package com.payapl.csdmp.sp.config

import com.payapl.csdmp.sp.config.ConfigUtils.RichConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import scala.collection.JavaConverters.asScalaSetConverter
import com.paypal.csdmp.sp.utils.JsonUtils._
import com.paypal.csdmp.sp.utils.readContentFromFile

import scala.reflect.ClassTag

object ConfigUtils {

  def apply(appArgs: SpSubmitArguments): ConfigUtils = new ConfigUtils(appArgs)

  implicit class RichConfig(val underlying: Config) extends AnyVal {

    def getOptionalString(value: String): Option[String] = {
      if (underlying.hasPath(value)) {
        Some(underlying.getString(value))
      } else {
        None
      }
    }

    def getOptionalBoolean(path: String): Option[Boolean] = {
      if (underlying.hasPath(path)) {
        Some(underlying.getBoolean(path))
      } else {
        None
      }
    }

    def getOptionalConfig(path: String): Option[Config] = {
      if (underlying.hasPath(path)) {
        Some(underlying.getConfig(path))
      } else {
        None
      }
    }

    def getOptionalMap(path: String): Option[Map[String, String]] = {
      if (underlying.hasPath(path)) {
        Some(underlying.getConfig(path).entrySet().asScala
          .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
          .toMap)
      } else {
        None
      }
    }

    def getOptionalList[T: ClassTag](path: String): Option[List[T]] = {
      if (underlying.hasPath(path)) {
        import scala.collection.JavaConversions._
        val stringType: ClassTag[String] = implicitly[ClassTag[String]]
        implicitly[ClassTag[T]] match {
          case `stringType` => Some(underlying.getStringList(path).toList.map(_.asInstanceOf[T]))
          case _ => Some(underlying.getObjectList(path).toList.map(_.unwrapped().toJson.as[T]))
        }
      } else {
        None
      }
    }
  }
}

class ConfigUtils(appArgs: SpSubmitArguments) {

  private val application = ConfigFactory.load()
  private val active: String = appArgs.env.getOrElse(application.getString("sp.profile.active"))
  private val activeConfig: Config = ConfigFactory.parseString(s"sp.profile.active=$active")
  private val config: Config = appArgs.externalConfigFileUrl match {
    case Some(url) => ConfigFactory.parseString(readContentFromFile(url))
      .withFallback(ConfigFactory.load(s"application-$active.conf"))
      .withFallback(activeConfig)
      .withFallback(application)
    case None => ConfigFactory.load(s"application-$active.config")
      .withFallback(activeConfig)
      .withFallback(application)
  }

  def getString(property: String) = config.getString(property)

  def getConfig(property: String) = config.getConfig(property)

  def getOptionalString(property: String) = config.getOptionalString(property)

  def getOptionalConfig(property: String) = config.getOptionalConfig(property)

  def getOptionalMap(property: String) = config.getOptionalMap(property)

  def getOptionalList[T: ClassTag](property: String) = config.getOptionalList[T](property)

  def getEnv = active

  override def toString: String = config.getConfig("sp").root().render(ConfigRenderOptions
    .defaults()
    .setOriginComments(false)
    .setComments(false)
    .setFormatted(true))
}