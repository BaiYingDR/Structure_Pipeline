package com.payapl.csdmp.sp.core.misc.offset.sotre

import com.payapl.csdmp.sp.core.misc.offset.sotre.OffsetEntity.TIME_PATTERN

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object OffsetEntity {
  val TIME_PATTERN: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def unapply(offsetEntity: String, sep: String = "\t"): OffsetEntity = {
    offsetEntity.split(sep) match {
      case Array(sourceId, consumerId, batchId, offset, updateTime) => OffsetEntity(sourceId, consumerId, batchId, offset, LocalDateTime.parse(updateTime))
      case _ => throw new RuntimeException(s"offset format is not valid")
    }
  }
}

case class OffsetEntity(sourceId: String,
                        consumerId: String,
                        batchId: String,
                        offset: String,
                        updateTime: LocalDateTime) {
  val sep = "\t"
  sourceId + sep + consumerId + sep + batchId + sep + offset + sep + updateTime.format(TIME_PATTERN)
}

case class Offset(batchId: String, offset: String, updateTime: Timestamp)