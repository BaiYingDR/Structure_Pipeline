package com.paypal.csdmp.sp.pojo

case class Pipeline(
      name: String,
      resource: Option[String],
      steps: Option[List[Map[String, Step]]],
      enable: Option[Boolean],
      initialSteps: Option[List[String]],
      finalSteps: Option[List[String]])
