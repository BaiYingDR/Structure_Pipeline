package com.payapl.csdmp.sp.core.validation

import com.payapl.csdmp.sp.core.Validation
import org.apache.spark.sql.SparkSession

case class ThresholdValidation(name: String,
                               actualDataFrame: String,
                               sql: String,
                               operator: String,
                               thresholdValue: Double
                              ) extends Validation {
  override def validate()(implicit sparkSession: SparkSession): Unit = {
   if (sparkSession.table(actualDataFrame).head(1).isEmpty) return
    val actualValue: Double = sparkSession.table(actualDataFrame).sqlContext.sql(sql).collect()(0)(0).toString.toDouble

    operator match {
      case "==" =>
        assert(actualValue == thresholdValue, s"validation $name failed,thresholdValue $thresholdValue threshold but got $actualValue value")
        logInfo(s"$name validation passed with $thresholdValue threshold and actualValue $actualValue value")
      case ">" =>
        assert(actualValue > thresholdValue, s"validation $name failed,thresholdValue > $thresholdValue threshold but got $actualValue value")
        logInfo(s"$name validation passed with $thresholdValue threshold and actualValue $actualValue value")
      case ">=" =>
        assert(actualValue >= thresholdValue, s"validation $name failed,thresholdValue >= $thresholdValue threshold but got $actualValue value")
        logInfo(s"$name validation passed with $thresholdValue threshold and actualValue $actualValue value")
      case "<" =>
        assert(actualValue < thresholdValue, s"validation $name failed,thresholdValue < $thresholdValue threshold but got $actualValue value")
        logInfo(s"$name validation passed with $thresholdValue threshold and actualValue $actualValue value")
      case "<=" =>
        assert(actualValue <= thresholdValue, s"validation $name failed,thresholdValue <= $thresholdValue threshold but got $actualValue value")
        logInfo(s"$name validation passed with $thresholdValue threshold and actualValue $actualValue value")
      case _ =>
        throw new RuntimeException(s"$operator is not support in count validation.")
    }
    
  }

}
