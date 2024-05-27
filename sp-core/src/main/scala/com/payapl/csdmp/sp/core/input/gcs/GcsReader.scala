package com.payapl.csdmp.sp.core.input.gcs

import com.payapl.csdmp.sp.core.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *  config to read from GCS path
 *
 * @param name
 * @param outputDataFrame
 * @param filePath
 * @param inferSchema
 * @param header
 * @param sep
 * @param encoding
 * @param multiline
 * @param quote
 * @param quoteAll
 * @param escape
 * @param escapeQuotes
 */
case class GcsReader(name: String,
                     outputDataFrame: Option[String],
                     filePath: String,
                     inferSchema: Option[Boolean],
                     header: Option[Boolean],
                     sep: Option[String],
                     encoding: Option[String],
                     multiline: Option[Boolean],
                     quote: Option[String],
                     quoteAll: Option[Boolean],
                     escape: Option[String],
                     escapeQuotes: Option[Boolean]
                    ) extends Reader {

  private val INFER_SCHEMA = "inferSchema"
  private val HEADER = "header"
  private val SEP = "sep"
  private val ENCODING = "encoding"
  private val MULTI_LINE = "multiline"
  private val QUOTE = "quote"
  private val QUOTE_ALL = "quoteAll"
  private val ESCAPE = "escape"
  private val ESCAPE_QUOTES = "escapeQuotes"

  override def read()(implicit sparkSession: SparkSession): DataFrame = {
    val inputDataFrame: DataFrame = sparkSession.read
      .option(INFER_SCHEMA, inferSchema.getOrElse(true))
      .option(HEADER, header.getOrElse(true))
      .option(SEP, sep.getOrElse(","))
      .option(ENCODING, encoding.getOrElse("UTF-8"))
      .option(MULTI_LINE, multiline.getOrElse(false))
      .option(QUOTE, quote.getOrElse("\""))
      .option(QUOTE_ALL, quoteAll.getOrElse(false))
      .option(ESCAPE, escape.getOrElse("\\"))
      .option(ESCAPE_QUOTES, escapeQuotes.getOrElse(false))
      .csv(filePath)
    inputDataFrame

  }
}
