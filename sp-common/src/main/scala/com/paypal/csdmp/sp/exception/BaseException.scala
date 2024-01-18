package com.paypal.csdmp.sp.exception

class BaseException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
