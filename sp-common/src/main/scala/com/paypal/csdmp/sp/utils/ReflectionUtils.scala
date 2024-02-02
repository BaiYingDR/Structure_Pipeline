package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.exception.MissingArgumentException
import com.paypal.csdmp.sp.utils.MapUtils.RichMap
import com.paypal.csdmp.sp.utils.JsonUtils._

import scala.reflect.api.{TypeCreator, Universe}
import scala.reflect.runtime.universe
import scala.reflect.{ClassTag, api, classTag}
import scala.reflect.runtime.universe._

object ReflectionUtils {


  def beanFromMap[T: TypeTag : ClassTag](m: Map[String, _]): T = {
    val rm: universe.Mirror = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val constructor: universe.MethodSymbol = typeOf[T].decl(termNames.CONSTRUCTOR).alternatives.head.asMethod
    val classSymbol: universe.ClassSymbol = typeOf[T].typeSymbol.asClass
    val module: universe.ModuleSymbol = classSymbol.companion.asModule
    val instanceMirror: universe.InstanceMirror = rm.reflect(rm.reflectModule(module).instance)

    val constructorArgs: Map[String, Any] = constructor.paramLists.flatten.zipWithIndex.map {
      case (param: Symbol, idx: Int) => {
        val paramName: String = param.name.toString
        val value: Any = if (param.typeSignature <:< typeOf[Option[Any]]) {
          m.getIgnoreCase(paramName)
        } getOrElse {
          m.getIgnoreCase(paramName).orElse(getDefaultValue(instanceMirror, param, idx))
            .getOrElse(throw new MissingArgumentException(s"missing required parameter [$paramName] in operator [${m("name")}]"))
        }
        paramName -> value
      }
    }.toMap
    constructorArgs.toJson.as[T]
  }

  def beanFromMap[T](m: Map[String, _], clazz: Class[T]): T = {
    val classTag: ClassTag[T] = ClassTag[T](clazz)
    val rm: universe.Mirror = runtimeMirror(clazz.getClassLoader)
    val tpe: universe.Type = rm.classSymbol(clazz).toType
    val typeCreator: TypeCreator = new TypeCreator {
      override def apply[U <: Universe with Singleton](m: api.Mirror[U]): U#Type = {
        if (m != rm) {
          throw new RuntimeException("wrong morror")
        } else tpe.asInstanceOf[U#Type]
      }
    }
    val typeTag: universe.TypeTag[T] = TypeTag[T](rm, typeCreator)
    beanFromMap(m)(typeTag, classTag)

  }

  def getDefaultValue(instanceMirror: InstanceMirror, param: universe.Symbol, idx: Int): Option[Any] = {
    if (param.asTerm.isParamWithDefault) {
      val ts: universe.Type = instanceMirror.symbol.typeSignature
      val defArg: universe.Symbol = ts.member(TermName(s"apply$$default$$${idx + 1}"))
      Some(instanceMirror.reflectMethod(defArg.asMethod))
    } else None
  }


}
