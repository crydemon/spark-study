package com.vova.export

import java.io.File
import java.util

import com.google.gson.Gson
import com.vova.db.DataSource
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
         | device_id,
         | idfv
         |FROM hit
         |WHERE  __time >= TIMESTAMP '$startTime'
         |    AND __time < TIMESTAMP '$endTime'
         |    $where
         |group by floor(__time to day), device_id, idfv
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
         | floor(__time to day) AS cur_day,
         | goods_id,
         | sum(clicks) AS sum_clicks,
         | sum(impressions) AS sum_impressions
         |FROM goods_ctr_v2
         |WHERE  __time >= TIMESTAMP '$startTime'
         |    AND __time < TIMESTAMP '$endTime'
         |    $where
         |group by  floor(__time to day), goods_id
        """.stripMargin
    println(sql)
    sql
  }

  def d_1334(spark: SparkSession): Unit = {
    import spark.implicits._
    val startTime = "2019-06-09 00:00:00"
    val endTime = "2019-06-11 00:00:00"
    Druid.loadData(Druid.query(fromGoodsCtrV2(startTime, endTime, "")), spark)
      .show()
  }


  def demand_1413(spark: SparkSession): Unit = {
    val clickPush =
      """
        |SELECT amp.push_time,
        |       aelmp.event_time,
        |       amp.user_id,
        |       amp.event_type
        |FROM app_message_push amp
        |         INNER JOIN app_event_log_message_push aelmp ON amp.id = aelmp.event_value
        |WHERE amp.push_result = 2
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND amp.push_time >= '2019-05-24'
        |  AND aelmp.event_type = 'click'
        |  AND amp.push_date >= '2019-05-24'
        |  AND aelmp.event_time >= '2019-05-24'
        |  AND aelmp.event_time >= amp.push_time
      """.stripMargin

    val tryPush =
      """
        |SELECT amp.push_time,
        |       amp.user_id,
        |       amp.event_type
        |FROM app_message_push amp
        |WHERE amp.push_result = 2
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND amp.push_time >= '2019-05-24'
      """.stripMargin

    val newOrder =
      """
        |SELECT loi.order_amount AS order_gmv,
        |       loi.create_time  AS order_time,
        |       loi.user_id,
        |       loi.pay_status,
        |       loi.pay_time
        |FROM luckystar_order_info loi
        |WHERE loi.create_time >= '2019-05-24'
      """.stripMargin

    val oldOrder =
      """
        |SELECT oi.order_amount AS order_gmv,
        |       oi.order_time,
        |       oi.user_id,
        |       oi.pay_status,
        |       oi.pay_time
        |FROM order_info oi
        |WHERE oi.order_time >= '2019-05-24'
        | and  exists(select * from order_extension oe where oe.order_id = oi.order_id and oe.ext_name = 'luckystar_activity_id')
      """.stripMargin

    //        val reportDb = new DataSource("themis_report_read")
    //        reportDb
    //          .load(spark, clickPush)
    //          .write
    //          .option("header", "true")
    //          .mode(SaveMode.Append)
    //          .csv("d:/click_push_csv/")
    //
    //        reportDb.load(spark, tryPush)
    //          .write
    //          .option("header", "true")
    //          .mode(SaveMode.Append)
    //          .csv("d:/try_push_csv/")
    //
    //        val themisDb = new DataSource("themis_read")
    //        themisDb.load(spark, newOrder)
    //          .write
    //          .option("header", "true")
    //          .mode(SaveMode.Append)
    //          .csv("d:/order_info_csv/")
    //
    //        themisDb.load(spark, oldOrder)
    //          .write
    //          .option("header", "true")
    //          .mode(SaveMode.Append)
    //          .csv("d:/order_info_csv/")

    import spark.implicits._

    spark.read
      .option("header", "true")
      .csv("D:\\click_push_csv\\")
      .withColumn("push_time", functions.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .withColumn("event_time", functions.regexp_replace($"event_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("click_push")

    spark.read
      .option("header", "true")
      .csv("d:/try_push_csv/")
      .withColumn("push_time", functions.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("try_push")


    spark.read
      .option("header", "true")
      .csv("D:\\order_info_csv")
      .withColumn("order_time", functions.regexp_replace($"order_time".substr(0, 19), "T", " "))
      .withColumn("pay_time", functions.regexp_replace($"pay_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("order_info")


    val tryOrderDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as try_order_user
        |from try_push tp
        | inner join order_info oi using(user_id)
        |where to_unix_timestamp(order_time) > to_unix_timestamp(push_time)
        | and to_unix_timestamp(order_time) - to_unix_timestamp(push_time) < 3600 * 24
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()


    val tryPayDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as try_pay_user,
        | sum(order_gmv) as try_pay_gmv
        |from (select first(push_time) as push_time, event_type, user_id from try_push group by date(push_time), event_type, user_id) tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(pay_time) > to_unix_timestamp(push_time)
        | and to_unix_timestamp(pay_time) - to_unix_timestamp(push_time) < 3600 * 24
        | and pay_status >=1
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()

    tryPayDf
      .show(false)


    val clickOrderDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as click_order_user
        |from click_push tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(order_time) > to_unix_timestamp(event_time)
        | and to_unix_timestamp(order_time) - to_unix_timestamp(event_time) < 3600 * 24
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()

    clickOrderDf
      .show(false)

    val clickPayDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as click_pay_user,
        | sum(order_gmv) as click_pay_gmv
        |from
        | (select
        |   first(push_time) as push_time, first(event_time) as event_time, event_type, user_id
        |   from click_push
        |   group by date(push_time), event_type, user_id) tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(pay_time) > to_unix_timestamp(event_time)
        | and to_unix_timestamp(pay_time) - to_unix_timestamp(event_time) < 3600 * 24
        | and pay_status >=1
        |group by date(push_time), event_type
      """.stripMargin)
    val data = tryOrderDf
      .join(tryPayDf, Seq("push_date", "event_type"))
      .join(clickOrderDf, Seq("push_date", "event_type"))
      .join(clickPayDf, Seq("push_date", "event_type"))
      .cache()

    clickPayDf
      .show(false)

    data.show(false)
    writeToCSV(data)
  }

  def merge(spark: SparkSession): Unit = {
    spark.read
      .option("header", "true")
      .csv("d:/lao.csv")
      .createOrReplaceTempView("lao")

    spark.read
      .option("header", "true")
      .csv("d:/lao.csv")
      .createOrReplaceTempView("xin")


    spark.sqlContext.sql(
      """
        |select
        | event_type,
        | push_date,
        | sum(l.try_order_user + x.try_order_user) as try_order_user,
        | sum(l.try_pay_user + x.try_pay_user) as try_pay_user,
        | sum(l.try_pay_gmv + x.try_pay_gmv) as try_pay_user,
        | sum(l.click_order_user + x.click_order_user) as click_order_user,
        | sum(l.click_pay_user + x.click_pay_user) as click_pay_user,
        | sum(l.click_pay_gmv + x.click_pay_gmv) as click_pay_user
        |from lao l
        | inner join xin x using(event_type, push_date)
        |group by event_type, push_date
      """.stripMargin)
  }

  def d_1467(spark: SparkSession): Unit = {
    import spark.implicits._
    val rawData = spark.read
      .option("header", "true")
      .csv("d:/flash_sale.csv")
      .filter($"os_family".isin("Android", "iOS"))
      .filter($"os_family" === "iOS")
      .withColumn("event_date", functions.to_date($"event_date"))
      .withColumn("device_id", $"idfv")


    val android = rawData

    val androidNext = rawData
      .withColumn("next_date", functions.date_sub($"event_date", 1))

    val androidCurDay = android
      .groupBy("event_date")
      .agg(
        functions.countDistinct("device_id").alias("cur_has_device")
      )

    val androidNextDay = android
      .join(androidNext, android("event_date") === androidNext("next_date") and (android("device_id") === androidNext("device_id")))
      .groupBy(android("event_date"))
      .agg(
        functions.countDistinct(android("device_id")).alias("next_has_device")
      )

    val androidNextDau = android
      .groupBy("event_date")
      .agg(
        functions.countDistinct("domain_userid").alias("next_dau")
      )
      .withColumn("next_day", functions.date_sub($"event_date", 1))
      .select("next_day", "next_dau")

    val data2 = androidCurDay
      .join(androidNextDay, Seq("event_date"), "left")
      .join(androidNextDau, androidCurDay("event_date") === androidNextDau("next_day"), "left")
      .withColumn("next_no_device", $"next_dau" - $"next_has_device")
      .select("event_date", "cur_has_device", "next_has_device", "next_no_device")
      .orderBy("event_date")
      .cache()

    writeToCSV(data2)


  }

  def d_1414(spark: SparkSession) = {
    import spark.implicits._
    val themisDb = new DataSource("themis_read")
    val reportDb = new DataSource("themis_report_read")
    val paySql =
      """
        |SELECT date(oi.pay_time)                           AS cur_day,
        |
        |       if(brand_id > 0, 'Y', 'N')                  AS is_brand,
        |       count(DISTINCT oi.user_id)                  AS pay_user,
        |       count(DISTINCT oi.order_id)                 AS pay_num,
        |       sum(og.shipping_fee + og.shop_price_amount) AS gmv
        |FROM order_info oi
        |         INNER JOIN region r ON r.region_id = oi.country
        |         INNER JOIN order_relation ore USING (order_id)
        |         INNER JOIN order_goods og USING (order_id)
        |         INNER JOIN goods g USING (goods_id)
        |         LEFT JOIN category c ON g.cat_id = c.cat_id
        |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
        |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
        |WHERE oi.pay_time >= '2019-06-27'
        |  AND oi.parent_order_id = 0
        |  AND oi.pay_status >= 1
        |  AND oi.from_domain LIKE 'api%'
        |  AND ore.device_type IN (11, 12)
        |GROUP BY cur_day, is_brand
      """.stripMargin

    val payDf = themisDb.load(spark, paySql)


    val payAllSql =
      """
        |SELECT date(oi.pay_time)                           AS cur_day,
        |
        |       'all'                  AS is_brand,
        |       count(DISTINCT oi.user_id)                  AS pay_user,
        |       count(DISTINCT oi.order_id)                 AS pay_num,
        |       sum(og.shipping_fee + og.shop_price_amount) AS gmv
        |FROM order_info oi
        |         INNER JOIN region r ON r.region_id = oi.country
        |         INNER JOIN order_relation ore USING (order_id)
        |         INNER JOIN order_goods og USING (order_id)
        |         INNER JOIN goods g USING (goods_id)
        |         LEFT JOIN category c ON g.cat_id = c.cat_id
        |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
        |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
        |WHERE oi.pay_time >= '2019-06-27'
        |  AND oi.parent_order_id = 0
        |  AND oi.pay_status >= 1
        |  AND oi.from_domain LIKE 'api%'
        |  AND ore.device_type IN (11, 12)
        |GROUP BY cur_day, is_brand
      """.stripMargin

    val payAllDf = themisDb.load(spark, payAllSql)

    val orderSql =
      """
        |SELECT date(oi.order_time)                           AS cur_day,
        |       if(brand_id > 0, 'Y', 'N')                  AS is_brand,
        |       count(DISTINCT oi.user_id)                  AS order_user,
        |       count(DISTINCT oi.order_id)                 AS order_num
        |FROM order_info oi
        |         INNER JOIN region r ON r.region_id = oi.country
        |         INNER JOIN order_relation ore USING (order_id)
        |         INNER JOIN order_goods og USING (order_id)
        |         INNER JOIN goods g USING (goods_id)
        |         LEFT JOIN category c ON g.cat_id = c.cat_id
        |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
        |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
        |WHERE oi.order_time >= '2019-06-27'
        |  AND oi.parent_order_id = 0
        |  AND oi.from_domain LIKE 'api%'
        |  AND ore.device_type IN (11, 12)
        |GROUP BY cur_day, is_brand
      """.stripMargin

    val orderDf = themisDb.load(spark, orderSql)

    val orderAllSql =
      """
        |SELECT date(oi.order_time)                           AS cur_day,
        |       'all'                  AS is_brand,
        |       count(DISTINCT oi.user_id)                  AS order_user,
        |       count(DISTINCT oi.order_id)                 AS order_num
        |FROM order_info oi
        |         INNER JOIN region r ON r.region_id = oi.country
        |         INNER JOIN order_relation ore USING (order_id)
        |         INNER JOIN order_goods og USING (order_id)
        |         INNER JOIN goods g USING (goods_id)
        |         LEFT JOIN category c ON g.cat_id = c.cat_id
        |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
        |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
        |WHERE oi.order_time >= '2019-06-27'
        |  AND oi.parent_order_id = 0
        |  AND oi.from_domain LIKE 'api%'
        |  AND ore.device_type IN (11, 12)
        |GROUP BY cur_day, is_brand
      """.stripMargin

    val orderAllDf = themisDb.load(spark, orderAllSql)

    val dau =
      s"""
         |SELECT tcdcd.action_date AS cur_day,
         |       count(1)          AS dau
         |FROM temp_country_device_cohort_details tcdcd
         |WHERE tcdcd.action_date >= '2019-06-27'
         |GROUP BY cur_day
        """.stripMargin
    val dauDf = reportDb
      .load(spark, dau)

    payDf
      .join(orderDf, Seq("cur_day", "is_brand"))
      .join(dauDf, "cur_day")
      .withColumn("user_pay_success_rate", functions.concat(functions.round($"pay_user" * 100.0 / $"order_user", 2), functions.lit("%")))
      .withColumn("order_pay_success_rate", functions.concat(functions.round($"pay_num" * 100.0 / $"order_num", 2), functions.lit("%")))
      .withColumn("order_rate", functions.concat(functions.round($"order_user" * 100.0 / $"dau", 2), functions.lit("%")))
      .withColumn("pay_rate", functions.concat(functions.round($"pay_user" * 100.0 / $"dau", 2), functions.lit("%")))
      .orderBy("cur_day")
      .show(false)

    payAllDf
      .join(orderAllDf, Seq("cur_day", "is_brand"))
      .join(dauDf, "cur_day")
      .withColumn("user_pay_success_rate", functions.concat(functions.round($"pay_user" * 100.0 / $"order_user", 2), functions.lit("%")))
      .withColumn("order_pay_success_rate", functions.concat(functions.round($"pay_num" * 100.0 / $"order_num", 2), functions.lit("%")))
      .withColumn("order_rate", functions.concat(functions.round($"order_user" * 100.0 / $"dau", 2), functions.lit("%")))
      .withColumn("pay_rate", functions.concat(functions.round($"pay_user" * 100.0 / $"dau", 2), functions.lit("%")))
      .orderBy("cur_day")
      .show(false)
  }

  def writeToCSV(data: DataFrame): Unit = {
    val savePath = "d:/result1"
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
      .config("spark.executor.cores", 2)
      .config("spark.sql.shuffle.partitions", 30)
      .config("spark.default.parallelism", 18)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    d_1467(spark)
    //merge(spark)
    spark.close()

  }


}