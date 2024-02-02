package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging

import java.io.{File, FileInputStream, FileOutputStream, FileWriter, InputStream}
import scala.io.Source

object FileUtils extends Logging{

  def read(path: String): String = Source.fromInputStream(new FileInputStream(path)).mkString

  def readOpt(path: String): Option[String] = {
    val file = new File(path)
    if (!file.isFile) None else Some(read(path))
  }

  def listDir(path: String): List[String] = {
    val dir = new File(path)
    if (dir.isDirectory) {
      dir.listFiles().map(_.getPath).toList
    } else throw new RuntimeException(s"$path is not directory")
  }

  def move(source: String, target: String): Boolean = {
    new File(source).renameTo(new File(target))
  }


  def cleanDir(path: String): Unit = {
    listDir(path).map(new File(_)).foreach(_.delete)
  }

  def mkdirIfNotExist(path: String): Unit = {
    val dirFile = new File(path)
    if (!dirFile.exists() || !dirFile.isDirectory) {
      dirFile.mkdir()
    }
  }


  def append(fileName: String, content: String): Unit = {
    write(fileName, content, true)
  }

  def overwrite(fileName: String, content: String): Unit = {
    write(fileName, content, false)
  }

  def write(fileName: String, content: String, appendMode: Boolean): Unit =
    Using(new FileWriter(fileName, appendMode)(_.append(content)))


  def write(fileName: String, input: InputStream): Unit = {
    val in = use(input)
    val out = use(new FileOutputStream(fileName))
    logInfo(s"begin to read stream to $fileName")
    IOUtils.copy(in, out)
    logInfo(s"write stream to $fileName successfully")

  }

}
