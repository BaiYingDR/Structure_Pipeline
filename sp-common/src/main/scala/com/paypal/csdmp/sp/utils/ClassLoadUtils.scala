package com.paypal.csdmp.sp.utils

object ClassLoadUtils {

  /**
   * get the ClassLoader which load Spark
   *
   * @return
   */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * get the context Classloader on this thread or, if not present, the ClassLoader that loaded spark
   *
   * this should be used whenever passing a classLoader to Class.forName() or finding the currently active loader when setting up ClassLoader delegation chains
   *
   * @return
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
   * Preferred alternative to Class.forName(className), as well as Class.forName(className,initialize,loader) with current thread's ContextClassLoader
   *
   * @param className
   * @return
   */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)\
  }
}
