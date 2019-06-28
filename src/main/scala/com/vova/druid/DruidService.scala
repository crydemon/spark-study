//package com.vova.druid
//
//import scala.collection.mutable.ListBuffer
//
//class DruidService {
//
//  private val sqlHost = "https://broker.eks.vova.com.hk/druid/v2/sql"
//  private val jsonHost = "https://broker.eks.vova.com.hk/druid/v2?prett"
//  private var queryJson = scala.collection.mutable.Map[String, _]()
//
//  def dataSource(ds: String): DruidService = {
//    queryJson = queryJson + ("dataSource" -> ds)
//    this
//  }
//
//  def queryType(qt: String): DruidService = {
//    queryJson = queryJson + ("queryType" -> qt)
//    this
//  }
//
//  def granularity(granularity: String): DruidService = {
//    queryJson = queryJson + ("granularity" -> granularity)
//    this
//  }
//
//  def addDimension(dimension: String): DruidService = {
//    queryJson
//      .getOrElse[ListBuffer[String]]("dimensions", new ListBuffer[String]())
//      .append(dimension)
//    this
//  }
//
//}
