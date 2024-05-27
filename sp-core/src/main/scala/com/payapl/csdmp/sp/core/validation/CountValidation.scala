package com.payapl.csdmp.sp.core.validation

import com.payapl.csdmp.sp.core.Validation
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CountValidation(name: String,
                           actualDataFrame: String,
                           compares: List[Map[String, Compare]] = Nil
                          ) extends Validation {


  override def validate()(implicit sparkSession: SparkSession): Unit = {

    val actualDF: DataFrame = sparkSession.table(actualDataFrame)
    val actual: Long = actualDF.count()

    compares.foreach(cp => {
      val compare: Compare = cp("compare")
      val operator: String = compare.operator.get
      val expected: Long = getValue(compare.value.get, sparkSession)
      operator match {
        case "==" =>
          assert(actual == expected, s"validation $name failed,expected $expected row but got $actual row")
          logInfo(s"$name validation passed with $expected rows and actual $actual rows")
        case ">" =>
          assert(actual > expected, s"validation $name failed,expected > $expected row but got $actual row")
          logInfo(s"$name validation passed with $expected rows and actual $actual rows")
        case ">=" =>
          assert(actual >= expected, s"validation $name failed,expected >= $expected row but got $actual row")
          logInfo(s"$name validation passed with $expected rows and actual $actual rows")
        case "<" =>
          assert(actual < expected, s"validation $name failed,expected < $expected row but got $actual row")
          logInfo(s"$name validation passed with $expected rows and actual $actual rows")
        case "<=" =>
          assert(actual <= expected, s"validation $name failed,expected <= $expected row but got $actual row")
          logInfo(s"$name validation passed with $expected rows and actual $actual rows")
        case _ =>
          throw new RuntimeException(s"$operator is not support in count validation.")
      }
    })
  }

  def getValue(value: Map[String, String], sparkSession: SparkSession): Long = {
    var count: Long = -1L
    value.get("dataframe").foreach(df => {
      val expectedDF: DataFrame = sparkSession.table(df)
      count = expectedDF.count()
    })
    value.get("constant").foreach(cnt => count = cnt.toLong)
    value.get("conf").flatMap(v => sparkSession.conf.getOption(v)).foreach(cnt => cnt.toLong)
    count
  }
}

case class Compare(operator: Option[String], value: Option[Map[String, String]])