package com.paypal.csdmp.sp.common.storage

import com.paypal.csdmp.sp.utils.HdfsUtils

import java.io.{InputStream, OutputStream}

trait HdfsStorageOperator extends StorageOperatorTrait {

  override val readInput: String => InputStream = path => HdfsUtils.readInputStream(path)

  override val writeOutput: String => OutputStream = path => HdfsUtils.createOutputStream(path)

  override val readOpt: String => Option[String] = path => HdfsUtils.readOpt(path)

  override val writeContent: (String, String) => Unit = (path, content) => HdfsUtils.overwrite(path, content)

  override val mkdirIfNotExist: String => Unit = path => HdfsUtils.mkdirIfNotExist(path)
}
