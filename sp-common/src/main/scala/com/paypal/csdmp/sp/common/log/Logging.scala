package com.paypal.csdmp.sp.common.log

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  @transient private var log_ : Logger = null;

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (log == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }



}
