package com.payapl.csdmp.sp.core.input.file

import com.payapl.csdmp.sp.core.Reader
import com.paypal.csdmp.sp.utils.{IOUtils, ResourceUtils, saveFile}
import com.paypal.csdmp.sp.utils.SchemaUtils.RichSchema
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.spark.sql.functions.{col, input_file_name, lit, regexp_extract, to_timestamp, udf}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class FileReader(name: String,
                      outputDataFrame: Option[String],
                      path: String,
                      format: String,
                      schema: Option[String],
                      options: Map[String, String] = Map.empty,
                      skip: Option[Int],
                      attacheFileName: Boolean = false,
                      attacheTimeStamp: Option[String],
                      schemaStorePath: Option[String],
                     ) extends Reader {


  override def read()(implicit sparkSession: SparkSession): DataFrame = {

    logInfo(s"format is $format, path is $path,options is $options")
    val originalDF: DataFrame = skip match {
      case Some(_) if path.startsWith("resource") => throw new IllegalArgumentException(s"skip parameter is not support for resource file")
      case Some(skipRowNum) =>
        val file: Array[String] = sparkSession.sparkContext.wholeTextFiles(path).map { case (filename, _) => filename }.collect()
        val dfs: Array[DataFrame] = file.map(fileName => {
          import sparkSession.implicits._
          val ds: Dataset[String] = sparkSession.sparkContext.textFile(fileName).mapPartitionsWithIndex { (idx, iteration) => if (idx == 0) iteration.drop(skipRowNum) else iteration }.toDS()
          val reader: DataFrameReader = sparkSession.read.format(format).options(options)
          schema.foreach(reader.schema)
          format match {
            case "csv" => reader.csv(ds)
            case "json" => reader.json(ds)
            case _ => throw new RuntimeException(s"$format format is not supported for skipping rows")
          }
        })
        dfs.reduce(_ union _)
      case None if path.startsWith("resource") =>
        val filePath: String = path.substring(11)
        import sparkSession.implicits._
        val data: Dataset[String] = ResourceUtils.readLines(filePath).toDS()
        val reader: DataFrameReader = sparkSession.read.options(options)
        schema.foreach(reader.schema)
        format match {
          case "csv" => reader.csv(data)
          case "json" => reader.json(data)
          case _ => throw new RuntimeException(s"$format format is not supported for resource file")
        }
      case None =>
        val reader: DataFrameReader = sparkSession.read.format(format).options(options)
        schema.foreach(reader.schema)
        reader.load(path)
    }

    val df: DataFrame = attacheFileNameToDf(originalDF)
    val res: DataFrame = attacheTimeStampToDF(df)
    logInfo(s"default schema for $path:${res.schema.toRdsDDL}")
    schemaStorePath.foreach(saveFile(_, IOUtils.toInputStream(res.schema.toRdsDDL)))
    res
  }

  def attacheFileNameToDf(df: DataFrame): DataFrame = {
    val getFileName = udf((fullpath: String) => FilenameUtils.getName(fullpath))
    if (attacheFileName) df.withColumn("filename", getFileName(input_file_name())) else df
  }

  def attacheTimeStampToDF(df: DataFrame): DataFrame = {
    attacheTimeStamp match {
      case Some("now") =>
        df.withColumn("evnt_ts", lit(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME)).cast("timestamp"))
      case Some(pattern) =>
        require(attacheFileName, s"filename parameter should be enabled for filename timestamp extraction.")
        val (regex, timeFormat): (String, String) = pattern.split("\\|") match {
          case Array(regex, timeFormat) => (regex, timeFormat)
          case _ => throw new IllegalArgumentException(s"pattern: $pattern is not valid, should in format regex|timeFormat")
        }
        df.withColumn("evnt_ts", to_timestamp(regexp_extract(col("filename"), regex, 0), timeFormat))
      case None => df
    }
  }
}
