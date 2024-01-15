package com.paypal.csdmp.sp.common.storage

import com.paypal.csdmp.sp.utils.GcsUtils

import java.io.{ InputStream, OutputStream}

trait GcsStorageOperator extends StorageOperatorTrait {

  override val readInput: String => InputStream = path => GcsUtils.readInputStream(path)

  override val writeOutput: String => OutputStream = path => GcsUtils.createOutputStream(path)

  override val readOpt: String => Option[String] = path => GcsUtils.readOpt(path)

  override val writeContent: (String, String) => Unit = (path, content) => GcsUtils.overwrite(path, content)

  override val mkdirIfNotExist: String => Unit = _ => {}
}
