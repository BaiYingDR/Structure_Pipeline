package com.payapl.csdmp.sp.core.output.csv

import com.payapl.csdmp.sp.core.Writer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

case class CsvWriter(name: String,
                     dataFrameName: String,
                     hdfsFolder: String,
                     fileName: Option[String],
                     delimiter: String,
                     header: String,
                     compression: String,
                     options: Option[Map[String, String]]
                    ) extends Writer {

  override def write(dataFrame: DataFrame): Unit = {
    val optionMap: Map[String, String] = Map("delimiter" -> delimiter, "header" -> header, "compression" -> compression) ++ options.getOrElse(Map())

    dataFrame.coalesce(1).write.options(optionMap).csv(hdfsFolder)

    if (fileName.isDefined) {
      val fs: FileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
      val file: String = fs.globStatus(new Path(s"$hdfsFolder/part*"))(0).getPath.getName
      val result: Boolean = fs.rename(new Path(s"$hdfsFolder/$file"), new Path(s"$hdfsFolder/${fileName.get}"))

      if (!result) {
        throw new RuntimeException("rename csv error")
      }
    }

  }
}
