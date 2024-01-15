package com.paypal.csdmp.sp.common.storage

import com.paypal.csdmp.sp.utils.FileUtils

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}

trait FileStorageOperator extends StorageOperatorTrait {

  override val readInput: String => InputStream = path => new FileInputStream(path)

  override val writeOutput: String => OutputStream = path => new FileOutputStream(path)

  override val readOpt: String => Option[String] = path => FileUtils.readOpt(path)

  override val writeContent: (String, String) => Unit = (path,content) => FileUtils.overwrite(path,content)

  override val mkdirIfNotExist: String => Unit = path =>FileUtils.mkdirIfNotExist(path)
}
