package com.paypal.csdmp.sp.utils

object StringUtils {

  def padLeft(s: String, len: Int, elem: Char): String = elem.toString * (len - s.length()) + s

  def removeSuffix(s: String, suffix: String): String = s.substring(0, s.length - (suffix.length + 1))

  implicit class RichString(str: String) {

    def padLeft(len: Int, elem: Char): String = StringUtils.padLeft(str, len, elem)

    def removeSuffix(suffix: String): String = str.substring(0, str.length - (suffix.length + 1))
  }

}
