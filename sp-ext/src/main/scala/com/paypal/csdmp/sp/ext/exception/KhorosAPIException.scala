package com.paypal.csdmp.sp.ext.exception

/**
 * @Author: Matthew Wu
 * @Date: 2023/1/9 14:45
 */
case class KhorosAPIException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
