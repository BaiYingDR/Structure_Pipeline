package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import scala.io.Source


object HdfsUtils extends Logging {

  private lazy val conf = new Configuration()
  private lazy val fs: FileSystem = FileSystem.get(conf)

  def setConf(map: Map[String, String]): Unit = map.foreach { case (k, v) => conf.set(k, v) }

  def read(path: String): String = read((new Path(path)))

  def readOpt(path: String): Option[String] = {
    val p = new Path(path)
    if (fs.isFile(p)) Some(read(p)) else None
  }

  def read(path: Path): String = Using(fs.open(path))(inputStream => Source.fromInputStream(inputStream).mkString)

  def readInputStream(path: String): InputStream = readInputStream(new Path(path))

  def readInputStream(path: Path): InputStream = fs.open(path)

  /**
   * delete file under dir in hdfs
   *
   * @param path
   */
  def cleanDir(path: String): Unit = {
    if (isDirectory(path)) {
      fs.delete(new Path(path), true)
      fs.mkdirs(new Path(path))
    } else {
      logWarning(s"$path could not be cleaned since it is a file not dir")
    }
  }

  /**
   * delete single file
   *
   * @param path
   * @return
   */
  def delete(path: String) = fs.delete(new Path(path), false)

  /**
   *
   * @param path
   * @return
   */
  def listDir(path: String) = {
    if (isDirectory(path)) {
      fs.listStatus(new Path(path)).filter(_.isFile).map(_.getPath.toString).toList
    } else {
      logWarning(s"$path could not be listed since it is a file not dir")
      List.empty[String]
    }
  }

  /**
   *
   * @param path
   * @param content
   */
  def write(path: String, content: String): Unit =
    Using(fs.create(new Path(path)))(_.write(content.getBytes()))


  def write(path: String, input: InputStream): Unit = Using.Manager { use =>
    val in = use(input)
    val out = use(fs.create(new Path(path)))
    logInfo(s"begin to read stream to $path")
    IOUtils.copy(in, out)
    logInfo(s"write stream to $path successfully")
  }

  def createOutputStream(path: String): FSDataOutputStream = fs.create(new Path(path))

  def mkdirIfNotExists(path: String): Unit = mkdirIfNotExist(new Path(path))

  def mkdirIfNotExist(path: Path): Unit = {
    if (!fs.isDirectory(path)) {
      logInfo(s"creating path $path ...")
      fs.mkdirs(path)
    }
  }

  def isDirectory(path: String): Boolean = fs.isDirectory(new Path(path))

  def exists(path: String): Boolean = fs.exists(new Path(path))

  def createNewFile(path: String): Boolean = fs.createNewFile(new Path(path))

  def readOutputStream(path: String): OutputStream = ???

  def writeObject[T](data: T, path: String): Unit = {
    Using(new ObjectOutputStream(readOutputStream(path)))(_.writeObject(data))
  }

  def readObject[T](data: T, path: String): Unit = {
    Using(new ObjectInputStream(readInputStream(path)))(_.readObject().asInstanceOf[T])
  }


}
