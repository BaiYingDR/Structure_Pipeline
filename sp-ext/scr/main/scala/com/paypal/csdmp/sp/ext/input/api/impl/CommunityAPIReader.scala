package com.paypal.csdmp.sp.ext.input.api.impl

@NoArgsConstructor
case class CommunityAPIReader (name: String,
                               outputDataFrame: Option[String],
                               query: String,
                               startOffset: Option[String],
                               batchId: Option[String],
                               zoneId: Option[String],
                               timeCol: Option[String],
                               idFrame: Option[String],
                               gcsPath: String
                              )extends Reader {

}
