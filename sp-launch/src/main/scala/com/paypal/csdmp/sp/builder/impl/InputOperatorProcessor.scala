package com.paypal.csdmp.sp.builder.impl

import com.payapl.csdmp.sp.core.Reader
import com.payapl.csdmp.sp.core.input.oracle.OracleReader
import com.paypal.csdmp.sp.builder.OperatorProcessorTrait
import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.factory.SpOperatorFactory
import com.paypal.csdmp.sp.pojo.Step
import org.apache.spark.sql.{DataFrame, SparkSession}

object InputOperatorProcessor extends OperatorProcessorTrait with Logging {

  override def process(step: Step)(implicit sparkSession: SparkSession): Unit = {
    logInfo("Building Input step: " + step.name)
    val reader: Reader = getReader(step.inputType.get.asInstanceOf[Map[String, Any]], step.name)
    registerDataFrame(reader)
  }

  private def getReader(inputType: Map[String, Any], stepName: String = "temp"): Reader = {
    inputType.map { case (name, params) => SpOperatorFactory.getInput(name, buildParams(stepName, params)) }.headOption
      .getOrElse(throw new RuntimeException("reader is not existed..."))
  }

  def registerDataFrame(input: Reader)(implicit sparkSession: SparkSession): Unit = {
    logInfo(s"Registering ${input.name} table")
    val df: DataFrame = input.read()
    logInfo("Print the schema from source's dataframe: ")
    df.printSchema()

    val dfRegisterName: String = input.outputDataFrame.getOrElse(input.name)
    df.createOrReplaceTempView(dfRegisterName)
    // cache input reader dataFrame
    val rowCountOpt: Option[Long] = if (cacheChecker(Some(dfRegisterName))) {
      df.cache()
      val rowCount: Long = df.count()
      logInfo(s"the source dataframe $dfRegisterName has been cached total $rowCount rows")
      Some(rowCount)
    } else None

    if (input.isInstanceOf[OracleReader]) {
      val rowCount: Long = rowCountOpt.getOrElse(df.count())
      sparkSession.conf.set(s"rds_cnt_validation", rowCount)
      logInfo(s"spark configuration value rds_cnt_validation has been set to value $rowCount for potential OracleReader validation")
    }
  }

}
