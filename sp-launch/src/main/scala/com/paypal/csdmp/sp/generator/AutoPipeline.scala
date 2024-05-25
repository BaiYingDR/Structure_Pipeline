package com.paypal.csdmp.sp.generator

import com.paypal.csdmp.sp.common.log.Logging
import org.apache.spark.sql.types.StructType

object AutoPipeline extends Logging {

  def buildBigQueryDDL(tableName: String, schema: StructType): String = {


    val ODSDDL: String =
      s"""
         |-- -------------------------------------
         |-- DDL for BigQuery ODS Table
         |-- -------------------------------------
         |Create Table `pypl-edw`.`pp_cs_ods`.`$tableName`(
         |  ${}
         |)
         |""".stripMargin
  }

  def mappingInfo(): String =
    s"""
       |
       |""".stripMargin

  def buildBigQueryExternalDDL(tableName: String, schema: StructType) = {

  }
}
