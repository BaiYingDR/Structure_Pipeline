package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 */
object RetryUtils extends Logging {

  class RetryBuilder[T] {
    var times = 3
    var interval = 5000
    var throwExceptionExceed: Boolean = false
    var exceptionIfExceed: Exception = _
    var retryIfFalse: T => Boolean = _
    var retryIfExceptionOfType: List[Class[_ <: Throwable]] = List.empty

    def setRetryTimes(times: Int): RetryBuilder[T] = {
      this.times = times
      this
    }

    def setRetryInterval(interval: Int): RetryBuilder[T] = {
      this.interval = interval
      this
    }

    def setThrowExceptioniIfExceed(t: Boolean): RetryBuilder[T] = {
      this.throwExceptionExceed = t
      this
    }

    def setExceptionIfExceed(e: Exception): RetryBuilder[T] = {
      this.exceptionIfExceed = e
      this
    }

    def setRetryIfFalse(f: T => Boolean): RetryBuilder[T] = {
      this.retryIfFalse = f
      this
    }

    def setRetryIfExceptionOfType(exceptionClasses: List[Class[_ <: Throwable]]): RetryBuilder[T] = {
      this.retryIfExceptionOfType = exceptionClasses
      this
    }

    def build(): Retry[T] = {
      new Retry[T](this.times, this.interval, this.throwExceptionExceed, this.exceptionIfExceed, this.retryIfFalse, this.retryIfExceptionOfType)
    }
  }


  case class Retry[T](times: Int, interval: Int, throwException: Boolean, exceptionIfExceed: Exception,
                      retryIfFalse: T => Boolean, retryIfExceptionOfType: List[Class[_ <: Throwable]]) {


    def run(callable: Callable[T]): Option[T] = {
      var result: Option[T] = None
      try {
        result = Some(callable.call())
        if (retryIfFalse != null && retryIfFalse(result.get)) retry(callable)
      } catch {
        case e: Exception if isIncludeException(e) =>
          logInfo("Execution failed, will begin to retry", e)
          retry(callable)
      }
    }


    def retry(callable: Callable[T]): Option[T] = {
      val curTimes = new AtomicInteger(1)
      var result: Option[T] = None
      var callException: Exception = null

      if (curTimes.get() <= times) {
        logInfo(s"start to retry in ${curTimes.get} times")
        try {
          result = Some(callable.call())
        } catch {
          case e: Exception => callException = e
        }

        if (callException != null) {
          if (isIncludeException(callException)) {
            logError("Retry is failed due to call() throw Exception", callException)
            logInfo("Continue to retry when exception are matched")
            result = Some(("Failed Request Due to: " + callException.toString).asInstanceOf[T])
          } else {
            if (retryIfFalse == null) {
              return result
            } else if (retryIfFalse(result.get)) {
              return result
            }
          }
          curTimes.incrementAndGet()
          Thread.sleep(interval)
        }
        if (throwException) {
          logError("we've tried " + times + "times, request still can't be successful!")
          if (exceptionIfExceed != null) {
            throw exceptionIfExceed
          } else
            throw callException
        }

      }
      result
    }

    def retryWithoutReturn(runnable: Runnable): Unit = {
      val curTimes = new AtomicInteger(1)
      if (curTimes.get() <= times) {
        try {
          runnable.run()
        } catch {
          case e: Exception =>
            if (isIncludeException(e)) {
              logInfo("Continue to retry when exception are matched")
            } else {
              logError("Retry is failed due to call() throw Exception", e)
            }
        }

        curTimes.incrementAndGet()
        Thread.sleep(interval)
      }

      if (throwException) {
        logError("we've tried " + times + "times, request still can't be successful!")
        throw exceptionIfExceed
      }
    }

    def isIncludeException(e: Exception): Boolean = {
      retryIfExceptionOfType.exists(e => e.isAssignableFrom(e.getClass))
    }
  }
}
