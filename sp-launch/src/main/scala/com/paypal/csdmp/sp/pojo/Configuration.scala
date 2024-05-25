package com.paypal.csdmp.sp.pojo

/**
 *
 * @param offset
 * @param pipelines
 *
 * example config in yaml file
 *
 * initialization:
 *  - initialization:
 *    name: Init_feature_reader
 *    featureRequestReader:
 *      param1: @etl_date
 *  pipelines:
 *    - pipeline:
 *     name:feature_engineering_poc
 *     initialStep:
 *      - ""
 *     finalSteps:
 *      - ""
 *     Steps:
 *      - step:
 */
case class Configuration(offset: Option[Offset], pipelines: Option[List[Map[String, Pipeline]]])
