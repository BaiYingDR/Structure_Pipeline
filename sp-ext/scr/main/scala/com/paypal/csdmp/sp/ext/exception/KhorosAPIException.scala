package com.paypal.csdmp.sp.ext.exception

case class KhorosAPIException (private val message: String = "",private val casue: Throwable=None.orNull) extends Exception(message,casue)

