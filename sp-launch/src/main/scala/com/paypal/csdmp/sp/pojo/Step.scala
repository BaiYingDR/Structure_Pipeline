package com.paypal.csdmp.sp.pojo

case class Step(name: String,
                inputType: Option[Any],
                outPutType: Option[Any],
                transformType: Option[Any],
                miscType: Option[Any],
                validationType: Option[Any],
                externalCallType: Option[Any]) {
  require(name != null, "step name should be provided for structured pipeline step")
  require((inputType.isDefined && outPutType.isEmpty && transformType.isEmpty
    || inputType.isEmpty && outPutType.isDefined && transformType.isEmpty
    || inputType.isEmpty && outPutType.isEmpty && transformType.isDefined)
    || miscType.isDefined
    || validationType.isDefined
    || externalCallType.isDefined,
    "Exactly one of (inputType, outPutType, transformType, miscType, validationType, externalCallType) must be defined"
  )
}
