package com.paypal.csdmp.sp.utils

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.`type`.CollectionType
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util
import scala.reflect.ClassTag

object JsonUtils {

  private val mapper = new ObjectMapper()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_ABSENT)
    .registerModule(DefaultScalaModule)

  implicit class obj2Json(obj: AnyRef) {
    def toJson: String = JsonUtils.toJson(obj)
  }

  implicit class Json2Obj(json: String) {
    def as[T: ClassTag]: T = {
      val vm: ClassTag[T] = implicitly[ClassTag[T]]
      JsonUtils.as[T](json, vm.runtimeClass.asInstanceOf[Class[T]])
    }

    def asList[T: ClassTag]: List[T] = {
      val vm: ClassTag[T] = implicitly[ClassTag[T]]
      JsonUtils.asList[T](json, vm.runtimeClass.asInstanceOf[Class[T]])
    }

    def readPath[T: ClassTag](path: String): T = {
      val jsonNode: JsonNode = mapper.readTree(json).at(path)
      val stringType: Any = implicitly[ClassTag[String]]
      val jsonNodeType: Any = implicitly[ClassTag[JsonNode]]
      val res: Any = implicitly[ClassTag[T]] match {
        case `stringType` => jsonNode.asText()
        case ClassTag.Int => jsonNode.asInt()
        case ClassTag.Long => jsonNode.asLong()
        case ClassTag.Boolean => jsonNode.asBoolean()
        case ClassTag.Double => jsonNode.asDouble()
        case `jsonNodeType` => jsonNode
        case _ => jsonNode.toJson.as[T]
      }
      res.asInstanceOf[T]
    }

  }

  def toJson(obj: AnyRef) = {
    mapper.writeValueAsString(obj)
  }

  def as[T](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }

  def asList[T](json: String, clazz: Class[T]): List[T] = {
    val listType: CollectionType = mapper.getTypeFactory.constructCollectionType(classOf[java.util.List[T]], clazz)
    import scala.collection.JavaConversions._
    val list: java.util.List[T] = mapper.readValue(json, listType)
    list.toList
  }

  def createJsonObject: ObjectNode = mapper.createObjectNode()
}
