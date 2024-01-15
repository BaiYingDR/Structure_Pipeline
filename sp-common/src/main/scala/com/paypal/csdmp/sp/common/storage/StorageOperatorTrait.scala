package com.paypal.csdmp.sp.common.storage

import java.io.{InputStream, OutputStream}


trait StorageOperatorTrait {

  val readInput: String => InputStream

  val writeOutput: String => OutputStream

  val readOpt: String => Option[String]

  val writeContent: (String, String) => Unit

  val mkdirIfNotExist: String => Unit

}
