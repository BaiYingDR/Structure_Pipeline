package com.paypal.csdmp.sp.utils

import scala.io.Source

object ResourceUtils {

  /**
   * read content from file in resource folder
   *
   * @param path a/b/c
   * @return
   */
  def read(path: String) = Source.fromInputStream(getClass.getResourceAsStream(s"/$path")).mkString

  def readLines(path: String) = Source.fromInputStream(getClass.getResourceAsStream(s"/$path")).getLines().toList


}
