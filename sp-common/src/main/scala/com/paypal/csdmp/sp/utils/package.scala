package com.paypal.csdmp.sp

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.SparkSession

import java.io.{InputStream, StringReader}
import java.lang.reflect.Modifier
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

package object utils extends Logging {

  def castPropsStringToMap(content: String): Map[String, String] = {
    val p = new Properties()
    p.load(new StringReader(content))
    p.asScala.toMap
  }

  def mergeOptionalMap(om1: Option[Map[String, String]], om2: Option[Map[String, String]]): Option[Map[String, String]] = {
    (om1, om2) match {
      case (Some(m1), Some(m2)) => Some(m2 ++ m1)
      case (Some(m1), None) => Some(m1)
      case (None, Some(m2)) => Some(m2)
      case (None, None) => None
    }
  }

  def readContentFromFile(fileName: String): String = {
    fileName match {
      case f if f.startsWith("hdfs") =>
        val path: String = URI.create(fileName).getPath
        logInfo(s"the hdfs file path is: $path")
        HdfsUtils.read(path)

      case f if f.startsWith("gs") => GcsUtils.read(fileName)
      case f if f.startsWith("resource") => ResourceUtils.read(fileName.substring(11))
      case _ => FileUtils.read(fileName)
    }
  }

  /**
   * @param path
   */
  def cleanDir(path: String): Unit = path match {
    case f if f.startsWith("hdfs://") => HdfsUtils.cleanDir(path)
    case f if f.startsWith("gs://") => GcsUtils.cleanDir(path)
    case _ => FileUtils.cleanDir(path)
  }

  def saveFile(path: String, is: InputStream): Any = path match {
    case f if f.startsWith("hdfs://") => HdfsUtils.write(path, is)
    case f if f.startsWith("gs://") => GcsUtils.write(path, is)
    case _ => FileUtils.write(path, is)
  }

  def listDir(path: String): List[String] = path match {
    case f if f.startsWith("hdfs://") => HdfsUtils.listDir(path)
    case f if f.startsWith("gs://") => GcsUtils.listDir(path)
    case _ => FileUtils.listDir(path)
  }

  def mkdir(path: String) = path match {
    case f if f.startsWith("hdfs://") => HdfsUtils.mkdirIfNotExists(path)
    case f if f.startsWith("gs://") =>
    case _ => FileUtils.listDir(path)
  }

  def checkEnv(path: String) = path match {
    case f if f.startsWith("hdfs://") => "hdfs"
    case f if f.startsWith("gs://") => "gcs"
    case _ => "file"
  }

  def matchPattern(fileName: String, fileNamePattern: Option[String]): Boolean = {
    fileNamePattern.map(p => {
      val pattern: String = p.replace(".", "\\.").replace("*", ".*")
      fileName.matches(pattern)
    }).getOrElse(true)
  }

  def parseContextInQuery(sql: String, sparkSession: SparkSession): String = {
    var query: String = sql
    if (query.contains("ctx_")) {
      val map: Map[String, String] = sparkSession.conf.getAll
      for (key <- map.keySet) {
        if (key.startsWith("ctx_")) {
          query = query.replaceAll(key, map(key))
        }
      }
    }
    query
  }

  def registerUDF(clazz: String)(implicit spark: SparkSession): Unit = {
    logInfo(s"register spark udf function from class $clazz ..")
    ClassLoaderUtils.classForName(clazz).getMethods.foreach { m =>
      if (Modifier.isStatic(m.getModifiers)) {
        m.invoke(null, spark.udf)
      }
    }
  }


}
