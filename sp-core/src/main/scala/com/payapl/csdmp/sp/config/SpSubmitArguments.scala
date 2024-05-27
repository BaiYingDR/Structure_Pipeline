package com.payapl.csdmp.sp.config

import org.slf4j.LoggerFactory
import scopt.OptionParser

import java.util.UUID

case class SpSubmitArguments(args: Seq[String]) {

  private val log = LoggerFactory.getLogger(getClass)
  var appName: String = _
  var ymlFile: String = _
  var variables: Option[Map[String, String]] = _
  var env: Option[String] = _
  var externalConfigFileUrl: Option[String] = _

  loadArgument()
  logArgument()


  private def logArgument() = {
    log.info(s"Load Submit Argument - appName: $appName")
    log.info(s"Load Submit Argument - yml: $ymlFile")
    log.info(s"Load Submit Argument - variables: $variables")
    log.info(s"Load Submit Argument - env: $env")
    log.info(s"Load Submit Argument - externalConfigFileUrl: $externalConfigFileUrl")
  }

  private def loadArgument(): Unit = {
    val commandArgsParser: OptionParser[CommandArgs] = new OptionParser[CommandArgs]("structure pipeline") {
      opt[String]('a', "appName").optional().action {
        (x, c) => c.copy(appName = Option(x))
      }.text("application anme")

      opt[String]('f', "yml").required().action {
        (x, c) => c.copy(appName = Option(x))
      }.text("yml file url")

      opt[String]('v', "variables").optional().action {
        (x, c) => c.copy(appName = Option(x))
      }.text("variable in yml to replace")

      opt[String]('e', "env").optional().action {
        (x, c) => c.copy(appName = Option(x))
      }.text("specify active env: qa(default),pre or prod")

      opt[String]('c', "config").optional().action {
        (x, c) => c.copy(appName = Option(x))
      }.text("external configuration file to included")

    }
    commandArgsParser.parse(args, CommandArgs()) match {
      case Some(config) =>
        appName = config.appName.getOrElse(generateRandomAppName)
        ymlFile = config.ymlFile
        variables = parseCommandVariable(config.variablesMap)
        env = config.env
        externalConfigFileUrl = config.externalConfigFileUrl
      case _ => throw new IllegalArgumentException("required parameter should be provided in command line !!!")
    }
  }

  private def parseCommandVariable(vars: Option[String]): Option[Map[String, String]] = {
    vars.map(
      _.split(",")
        .filter(_.trim.nonEmpty)
        .map(_.trim.split("\\^"))
        .map(kv => kv(0).trim -> kv(1).trim)
        .toMap
    )
  }

  private def generateRandomAppName() = s"sp-${UUID.randomUUID()}"

  case class CommandArgs(appName: Option[String] = None,
                         mode: String = "external",
                         ymlFile: String = "",
                         variablesMap: Option[String] = None,
                         env: Option[String] = None,
                         externalConfigFileUrl: Option[String] = None
                        )
}



