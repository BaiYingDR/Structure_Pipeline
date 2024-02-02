package com.paypal.csdmp.sp.utils

import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.{SqlBaseLexer, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{ColTypeContext, ColTypeListContext, SingleTableSchemaContext}

import scala.collection.JavaConverters.asScalaBufferConverter

object ParserUtils {


  def parseTableSchema(sqlText: String): Map[String, String] = parse(sqlText.toUpperCase())(parser =>
    visitSingleTableSchema(parser.singleTableSchema())
  )


  private def visitSingleTableSchema(ctx: SingleTableSchemaContext): Map[String, String] = {
    withOrigin(ctx)(visitColTypeList(ctx.colTypeList()))
  }

  private def visitColTypeList(ctx: ColTypeListContext): Map[String, String] = {
    ctx.colType().asScala.map(visitColType).toMap
  }

  private def visitColType(ctx: ColTypeContext): (String, String) = withOrigin(ctx) {
    (ctx.getChild(0).getText.toUpperCase(), ctx.dataType.getText.toUpperCase)
  }


  def parse[T](command: String)(toResult: SqlBaseParser => T): T = {

    val lexer = new SqlBaseLexer(CharStreams.fromString(command))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
    toResult(parser)
  }

}
