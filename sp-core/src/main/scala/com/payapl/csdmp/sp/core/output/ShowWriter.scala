package com.payapl.csdmp.sp.core.output

import com.payapl.csdmp.sp.core.Writer
import org.apache.spark.sql.DataFrame

case class ShowWriter(name: String, dfName: String, limit: Int = 20) extends Writer {

  override val dataFrameName: String = dfName

  override def write(dataFrame: DataFrame): Unit = dataFrame.show(limit, false)
}
