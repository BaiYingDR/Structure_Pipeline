package com.paypal.csdmp.sp.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag


object YamlUtils {

  private val mapper = new ObjectMapper(new YAMLFactory())
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  implicit class Yaml2obj(yaml: String) {
    def as[T: ClassTag] = {
      val vm = implicitly[ClassTag[T]]
      YamlUtils.as[T](yaml, vm.runtimeClass.asInstanceOf[Class[T]])
    }
  }

  def as[T](yaml: String, clazz: Class[T]): T = {
    mapper.readValue(yaml, clazz)
  }
}
