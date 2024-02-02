package com.paypal.csdmp.sp.utils

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, functions}

object SparkFunctionUtils {

  val SORT_FUNCTION_MAP: Map[String, Function[String, Column]] = Map(
    "asc_nulls_last" -> functions.asc_nulls_last,
    "asc_nulls_first" -> functions.asc_nulls_first,
    "desc_nulls_last" -> functions.desc_nulls_last,
    "asc" -> functions.asc,
    "desc" -> functions.desc
  )

  def getWindow(partitionCol: Option[List[String]], start: Long, end: Long, sort: Option[Column]): WindowSpec = {
    (partitionCol, sort) match {
      case (Some(h :: t), Some(s)) => Window.partitionBy(h, t: _*).orderBy(s).rangeBetween(start, end)
      case (Some(h :: t), None) => Window.partitionBy(h, t: _*).rangeBetween(start, end)
      case (None, Some(s)) => Window.orderBy(s).rangeBetween(start, end)
      case (None, None) => null
    }
  }

  def getWindow(partitionCol: Option[List[String]], sort: Option[List[Column]], isUnbounded: Boolean = false): WindowSpec = {
    (partitionCol, sort, isUnbounded) match {
      case (Some(h :: t), Some(s), true) => Window.partitionBy(h, t: _*).orderBy(s: _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      case (Some(h :: t), Some(s), false) => Window.partitionBy(h, t: _*).orderBy(s: _*)
      case (Some(h :: t), None, false) => Window.partitionBy(h, t: _*)
      case (None, Some(h), false) => Window.orderBy(h: _*)
      case (None, None, false) => null
    }
  }

  def getWindowByColumn(partitionCol: Option[List[Column]], sort: Option[List[Column]], isUnbounded: Boolean = false): WindowSpec = {
    (partitionCol, sort, isUnbounded) match {
      case (Some(t), Some(s), true) => Window.partitionBy(t: _*).orderBy(s: _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      case (Some(t), Some(s), false) => Window.partitionBy(t: _*).orderBy(s: _*)
      case (Some(t), None, false) => Window.partitionBy(t: _*)
      case (None, Some(s), false) => Window.orderBy(s: _*)
      case (None, None, false) => null
    }
  }

  def addSorts(window: WindowSpec, sortCols: Option[List[Column]]): WindowSpec = {
    sortCols match {
      case Some(h) => window.orderBy(h: _*)
      case None => window
    }
  }

  def getColType(dataFrame: DataFrame, columnName: String): String = {
    var dataType: String = dataFrame.schema(columnName).dataType.simpleString
    if (dataType.equals("long")) {
      dataType = "bigInt"
    } else if (dataType.equals("integer")) {
      dataType = "int"
    } else if (dataType.startsWith("decimal", 0)) {
      dataType = "decimal"
    }
    dataType
  }

}
