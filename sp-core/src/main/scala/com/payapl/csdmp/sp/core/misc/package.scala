package com.payapl.csdmp.sp.core

import com.payapl.csdmp.sp.config.SpContext
import com.paypal.csdmp.sp.consts.Constant
import com.paypal.csdmp.sp.utils.readContentFromFile

package object misc {

  def getCredentialFromUserInput(value: String): String = {
    if (value.startsWith("km:") || value.startsWith("km#")) {
      SpContext.getCredentialFromKeyMaker(value)
    } else if (
      value.startsWith(Constant.GCS_PATH_PREFIX) ||
        value.startsWith(Constant.HDFS_PATH_PREFIX) ||
        value.startsWith(Constant.RESOURCE_PATH_PREFIX) ||
        value.startsWith("/")
    ) {
      readContentFromFile(value)
    } else value
  }

}
