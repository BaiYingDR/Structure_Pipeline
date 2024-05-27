package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import org.apache.spark.sql.SparkSession

case class KafkaPPDeserTransformer() extends Transformer {

  override val name: String = _

  override def run()(implicit sparkSession: SparkSession): Unit = ???

  override def getOutputDfName(): Option[String] = ???
}
