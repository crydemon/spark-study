package com.vova.export

import java.io.File
import java.util

import com.google.gson.Gson
import org.apache.commons.io.FileUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object Druid {

  def query(druidSql: String): String = {

    val map = new util.HashMap[String, String]
    map.put("query", druidSql)
    val gson = new Gson()
    val json = gson.toJson(map)
    val post = new HttpPost("https://broker.eks.vova.com.hk/druid/v2/sql")
    val entity = new StringEntity(json)
    post.setEntity(entity)
    post.setHeader("Accept", "application/json")
    post.setHeader("Content-type", "application/json")
    val client = new DefaultHttpClient
    val response = client.execute(post)
    EntityUtils.toString(response.getEntity, "UTF-8")

  }

  def loadData(json: String, spark: SparkSession): DataFrame = {
    val response = json
    import spark.implicits._
    val endIndex = response.lastIndexOf(']')
    val normal = response.substring(1, endIndex).replace("},{", "}\n{")
    spark
      .read
      .json(normal.split("\n").toSeq.toDS)
    //.withColumn("pay_date", $"cur_day".substr(0, 10))
  }

}


object DruidSQL {
  def fromHit(startTime: String, endTime: String, where: String): String = {
    val sql =
      s"""
         |SELECT
         | floor(__time to day) AS cur_day,
         | count(DISTINCT domain_userid) AS uv,
         | count(1)  AS pv
         |FROM hit
         |WHERE  __time >= TIMESTAMP '$startTime'
         |    AND __time < TIMESTAMP '$endTime'
         |    $where
         |group by floor(__time to day)
        """.stripMargin
    println(sql)
    sql
  }

  def hourFromHit(startTime: String, endTime: String, where: String): String = {
    val sql =
      s"""
         |SELECT
         | floor(__time to day) AS cur_day,
         | floor(__time to hour) AS cur_hour,
         | count(DISTINCT domain_userid) AS uv,
         | count(1)  AS pv
         |FROM hit
         |WHERE  __time >= TIMESTAMP '$startTime'
         |    AND __time < TIMESTAMP '$endTime'
         |    $where
         |group by floor(__time to day), floor(__time to hour)
        """.stripMargin
    println(sql)
    sql
  }

  def fromGoodsCtrV2(startTime: String, endTime: String, where: String): String = {
    val sql =
      s"""
         |SELECT
         | goods_id,
         | sum(clicks) AS sum_clicks,
         | sum(impressions) AS sum_impressions
         |FROM goods_ctr_v2
         |WHERE  __time >= TIMESTAMP '$startTime'
         |    AND __time < TIMESTAMP '$endTime'
         |    $where
         |group by goods_id
        """.stripMargin
    println(sql)
    sql
  }

  def d_1334(spark: SparkSession): Unit = {
    import spark.implicits._
    val startTime = "2019-05-01 00:00:00"
    val endTime = "2019-06-11 00:00:00"
    val where = " and page_code = 'newuser_exclucivepage'"
    val d1 = Druid.loadData(Druid.query(fromGoodsCtrV2(startTime, endTime, where)), spark)
      .cache()
    d1.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", true)
      .csv("d:/d_1334/")
  }

  def d_1238(spark: SparkSession): Unit = {
    import spark.implicits._
    val startTime = "2019-04-24 00:00:00"
    val endTime = "2019-05-25 00:00:00"

    val goodsSale = spark
      .read
      .option("header", "true")
      .csv("d:/midden_sale.csv")

    goodsSale.cache()


    val goodsIds = goodsSale
      .map(row => row.getAs[String]("goods_id"))
      .collectAsList()
      .toArray.mkString(",")

    var where =
      s"""
         | and goods_id in($goodsIds)
         | and country in('SA', 'AE', 'BH', 'QA', 'KW')
      """.stripMargin

    val d1 = Druid.loadData(Druid.query(fromGoodsCtrV2(startTime, endTime, where)), spark)

    d1.cache()
    goodsSale
      .withColumnRenamed("goods_id", "goods_id1")
      .join(d1, $"goods_id" === $"goods_id1", "left")
      .withColumn("ctr", functions.concat($"sum_clicks" * 100 / $"sum_impressions", functions.lit("%")))
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", true)
      .csv("d:/d_1238/")


  }

  def d_1196(spark: SparkSession): Unit = {
    val startTime = "2019-05-13 00:00:00"
    val endTime = "2019-05-21 00:00:00"
    //1196
    var where =
      """
        | and page_code = 'lucky_star'
        | and element_name = 'Luckystarresulltpop'
        | and element_content = 'status=0'
      """.stripMargin


    val d1 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "not_reward_pop")

    where =
      """
        | and page_code = 'lucky_star_results'
        |  and element_content = 'status=2'
      """.stripMargin

    val d2 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "not_reward_page")
    where =
      """
        | and page_code = 'lucky_star'
        | and element_name = 'Luckystarresulltpop'
        | and element_content = 'status=1'
      """.stripMargin
    val d3 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "reward_pop")

    where =
      """
        | and page_code = 'lucky_star_results'
        | and element_content = 'status=2'
      """.stripMargin
    val d4 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "reward_page")


    val data = d1.join(d2, "cur_day")
      .join(d3, "cur_day")
      .join(d4, "cur_day")

    data.cache()
    writeToCSV(data)
  }

  def d_1214(spark: SparkSession): Unit = {
    val startTime = "2019-05-16 00:00:00"
    val endTime = "2019-05-22 00:00:00"
    var where =
      """
        | and page_code = 'auction_auctionhouse'
        | and event_name = 'page_view'
        | and country in('DE', 'ES')
      """.stripMargin

    val d1 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "auction_house_uv")


    //新人教程
    where =
      """
        | and element_name = 'auctionNewUsersCourseImp'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click_imp'
        | and element_content = 'auctionhouse'
        | and country in('DE', 'ES')
      """.stripMargin

    val d2 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "tutorial_hall_uv")

    where =
      """
        | and element_name = 'auctionNewUsersCourseClose'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click'
        | and element_content = 'auctionHouse'
      """.stripMargin

    val d3 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "tutorial_close_hall_uv")

    where =
      """
        | and page_code = 'auction_rules'
        | and event_name = 'page_view'
        | and country in('DE', 'ES')
      """.stripMargin

    val d4 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "rules_uv")

    where =
      """
        | and element_name = 'auctionNewUsersCourseClose'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click'
        | and element_content = 'auctionRules'
        | and country in('DE', 'ES')
      """.stripMargin

    val d5 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "tutorial_rules_uv")

    where =
      """
        | and element_name = 'auctionNewUsersCourseReplayImp'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click_imp'
        | and country in('DE', 'ES')
      """.stripMargin
    val d6 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "replay_uv")

    where =
      """
        | and page_code = 'auction_rules'
        | and event_name = 'page_view'
      """.stripMargin

    val d7 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "rules_all_country_uv")

    where =
      """
        | and element_name = 'auctionNewUsersCourseClose'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click'
        | and element_content = 'auctionRules'
      """.stripMargin

    val d8 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "tutorial_rules_all_country_uv")

    where =
      """
        | and element_name = 'auctionNewUsersCourseReplayImp'
        | and event_name = 'common_click'
        | and list_uri = 'auction_click_imp'
      """.stripMargin
    val d9 = Druid.loadData(Druid.query(fromHit(startTime, endTime, where)), spark)
      .select("uv", "cur_day")
      .withColumnRenamed("uv", "replay_all_country_uv")
    val data = d1.join(d2, "cur_day")
      .join(d3, "cur_day")
      .join(d4, "cur_day")
      .join(d5, "cur_day")
      .join(d6, "cur_day")
      .join(d7, "cur_day")
      .join(d8, "cur_day")
      .join(d9, "cur_day")

    data.cache()
    writeToCSV(data)
  }


  def d_1249(spark: SparkSession): Unit = {
    import spark.implicits._
    val startTime = "2019-05-20 00:00:00"
    val endTime = "2019-05-25 00:00:00"

    var where =
      """
        | and page_code = 'homepage'
        | and platform = 'mob'
        | and country in('SA', 'AE', 'BH', 'QA', 'KW')
      """.stripMargin

    val d1 = Druid.loadData(Druid.query(hourFromHit(startTime, endTime, where)), spark)
    d1.cache()
    val d2 = d1.withColumn("cur_day", functions.to_date($"cur_day"))
      .withColumn("cur_hour", functions.hour($"cur_hour"))

    val samples = d2.sample(false, 0.1)

    writeToCSV(d2)
  }

  def any_1(spark: SparkSession): Unit = {
    import spark.implicits._
    val newOld = spark.read.option("header", true)
      .csv("d:/new_old.csv")
      .withColumnRenamed("luckystar_order_id", "old_order_id").cache()
    val newNew = spark.read
      .option("header", true).csv("d:/new_new.csv").cache()
    println(newOld.count() - newNew.count())
    val d_1 = newOld
      .join(newNew, $"luckystar_order_id" === $"old_order_id", "left")
      .filter($"luckystar_order_id".isNull)
    d_1.show(20000, truncate = false)
  }

  def writeToCSV(data: DataFrame): Unit = {
    val savePath = "d:/result"
    FileUtils.deleteDirectory(new File(savePath))
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .option("delimiter", ",")
      .csv(savePath)
  }

  def main(args: Array[String]): Unit = {
    val appName = "druid"
    println(appName)
    val spark = SparkSession.builder
      .appName(appName)
      .master("local[4]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    d_1334(spark)
    spark.close()

  }


}