package com.payapl.csdmp.sp.core.trasnform

import com.payapl.csdmp.sp.core.Transformer
import com.paypal.csdmp.sp.utils.GcsUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

case class CheckPointOperator(name: String,
                              checkPointPath: String,
                              actionType: String
                              ) extends Transformer {

  override def run()(implicit sparkSession: SparkSession): Unit = {

    if (actionType.equals("INIT")) {
      sparkSession.sparkContext.setCheckpointDir(checkPointPath)
    }
    else if (actionType.equals("DELETE")) {
      deleteFile(checkPointPath, sparkSession)
    }
  }

  def deleteFile(path: String, sc: SparkSession): Unit = {
    if (path.startsWith("gs")) {
      if (GcsUtils.exists(path)) {
        GcsUtils.deleteObject(path)
      }
      return
    }

    val fs = FileSystem.get(sc.sparkContext.hadoopConfiguration)
    val outputPath = new Path(path)

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
  }

  override def getOutputDfName(): Option[String] = None
}
