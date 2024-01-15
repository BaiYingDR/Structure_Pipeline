package com.paypal.csdmp.sp.common.log

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  @transient private var log_ : Logger = null

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (log == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  protected def logInfo(msg: => Any): Unit = {
    if (log.isInfoEnabled) log.info(msg.toString)
  }

  protected def logDebug(msg: => Any): Unit = {
    if (log.isInfoEnabled) log.debug(msg.toString)
  }

  protected def logTrace(msg: => Any): Unit = {
    if (log.isInfoEnabled) log.trace(msg.toString)
  }

  protected def logWarning(msg: => Any): Unit = {
    if (log.isInfoEnabled) log.warn(msg.toString)
  }

  protected def logError(msg: => Any): Unit = {
    if (log.isInfoEnabled) log.error(msg.toString)
  }

  protected def logInfo(msg: => Any, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(msg.toString, throwable)
  }

  protected def logDebug(msg: => Any, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.debug(msg.toString, throwable)
  }

  protected def logTrace(msg: => Any, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.trace(msg.toString, throwable)
  }

  protected def logWarning(msg: => Any, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.warn(msg.toString, throwable)
  }

  protected def logError(msg: => Any, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.error(msg.toString, throwable)
  }
}
