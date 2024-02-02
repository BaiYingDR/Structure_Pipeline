package com.paypal.csdmp.sp.utils

import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.StructType

object SchemaUtils {

  implicit class RichSchema(schema: StructType) {

    def toRdsDDL(): String = schema.fields.map(f => s"${quoteIdentifier(f.name.toLowerCase)} ${f.dataType.sql}").mkString(",")

  }

}
