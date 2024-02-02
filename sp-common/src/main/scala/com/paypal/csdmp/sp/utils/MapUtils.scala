package com.paypal.csdmp.sp.utils

object MapUtils {

  def getIgnoreCase[T](map: Map[String, T], key: String) = {
    map.find {
      case (k, _) => k.equalsIgnoreCase(key)
    }.map {
      case (_, v) => v
    }
  }

  implicit class RichMap[T](map: Map[String, T]) {

    def getIgnoreCase(key: String) = {
      map.find {
        case (k, _) => k.equalsIgnoreCase(key)
      }.map {
        case (_, v) => v
      }
    }
  }


}
