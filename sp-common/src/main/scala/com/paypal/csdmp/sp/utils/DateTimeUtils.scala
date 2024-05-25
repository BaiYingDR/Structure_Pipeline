package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateTimeUtils extends Logging {

  def getCurrentTimestampStr: String = {
    val format = "yyyy-MM-dd HH:mm:ss"
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val ldt: LocalDateTime = LocalDateTime.now()
    ldt.format(dtf)
  }

  def getCurrentUTCTimestampStr: String = {
    val format = "yyyy-MM-dd HH:mm:ss"
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val ldt: LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))
    ldt.format(dtf)
  }

  def formatStrTimestamp(str: String): String = {
    try {
      var newStr: String = str
      if (str.contains(".")) {
        newStr = str.split("\\.")(0)
      }
      val format = "yyyy-MM-dd HH:mm:ss"
      val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
      val ts: LocalDateTime = LocalDateTime.parse(newStr, dtf)
      ts.format(dtf)
    } catch {
      case e: Exception => logError("DateTime formatting failed, exception caught", e)
        throw e
    }
  }

  def getEpochTime(ts: LocalDateTime): Long = ts.toEpochSecond(ZoneOffset.UTC)
}
