package com.vova.export

import java.io.File
import java.util

import com.google.gson.Gson
import com.vova.conf.Conf
import com.vova.db.DataSource
import org.apache.commons.io.FileUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => F}

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
    //    val luckystarFeedsGoodsC =
    //      """
    //        |SELECT
    //        |   floor(__time to day) as cur_day,
    //        |   country,
    //        |   count(DISTINCT domain_userid) as LuckystarFeedsGoods
    //        |FROM hit
    //        |WHERE page_code = 'lucky_star'
    //        |    AND element_name = 'LuckystarFeedsGoods'
    //        |    AND event_name = 'common_click'
    //        |    AND __time >= '2019-06-10'
    //        |    AND country IN('FR', 'GB','DE','IT','ES','PT')
    //        |GROUP BY floor(__time to day), country
    //      """.stripMargin
    //    val fgc =  Druid.loadData(Druid.query(luckystarFeedsGoodsC), spark)
    //
    //    val luckystarHighOddsGoodsc =
    //      """
    //        |SELECT
    //        |   floor(__time to day) as cur_day,
    //        |   country,
    //        |   count(DISTINCT domain_userid) as LuckystarHighOddsGoods
    //        |FROM hit
    //        |WHERE page_code = 'lucky_star'
    //        |    AND element_name = 'LuckystarHighOddsGoods'
    //        |    AND event_name = 'common_click'
    //        |    AND __time >= '2019-06-10'
    //        |    AND country IN('FR', 'GB','DE','IT','ES','PT')
    //        |GROUP BY floor(__time to day), country
    //      """.stripMargin
    //    val ogc =  Druid.loadData(Druid.query(luckystarHighOddsGoodsc), spark)
    //
    //    val luckystarBrandGoodsc =
    //      """
    //        |SELECT
    //        |   floor(__time to day) as cur_day,
    //        |   country,
    //        |   count(DISTINCT domain_userid) as LuckystarBrandGoods
    //        |FROM hit
    //        |WHERE page_code = 'lucky_star'
    //        |    AND element_name = 'LuckystarBrandGoods'
    //        |    AND event_name = 'common_click'
    //        |    AND __time >= '2019-06-10'
    //        |    AND country IN('FR', 'GB','DE','IT','ES','PT')
    //        |GROUP BY floor(__time to day), country
    //      """.stripMargin
    //    val bgc =  Druid.loadData(Druid.query(luckystarBrandGoodsc), spark)
    //
    //    val exposureUvc =
    //      """
    //        |SELECT
    //        |   floor(__time to day) as cur_day,
    //        |   country,
    //        |   count(DISTINCT domain_userid) as exposure_uv
    //        |FROM hit
    //        |WHERE page_code = 'lucky_star'
    //        |    AND event_name = 'page_view'
    //        |    AND __time >= '2019-06-10'
    //        |    AND country IN('FR', 'GB','DE','IT','ES','PT')
    //        |GROUP BY floor(__time to day), country
    //      """.stripMargin
    //    val euc =  Druid.loadData(Druid.query(exposureUvc), spark)
    //
    //    val data = euc
    //      .join(bgc, Seq("cur_day", "country"))
    //      .join(ogc, Seq("cur_day", "country"))
    //      .join(fgc, Seq("cur_day", "country"))
    //
    //    writeToCSV(data)


    //表二
    val winSpec = Window
      .partitionBy("user_id")
      .orderBy("pay_time")
    val loi_1 = spark
      .read
      .option("header", "true")
      .csv("d:/luckystar_order_info.csv")
      .withColumn("event_date", F.to_date($"pay_time"))


    val firstOrder = loi_1
      .filter($"pay_status" >= 1)
      .withColumn("first_pay_time", F.first("pay_time", true).over(winSpec))
      .select("user_id", "first_pay_time")
      .distinct()


    val loi = loi_1
      .join(firstOrder, Seq("user_id"), "left")
      .withColumn("diff", F.datediff($"pay_time", $"first_pay_time"))
      .withColumn("user_tag", F.when($"diff" === 0 or ($"first_pay_time".isNull), "new").otherwise("old"))

    val users = spark
      .read
      .option("header", "true")
      .csv("d:/lucky_star_users.csv")


    //找到老用户
    val oldUv = users
      .withColumn("event_date", F.to_date($"event_date"))
      .join(firstOrder, $"user_id" === $"user_unique_id" and ($"event_date" > $"first_pay_time"), "left")

      .groupBy("event_date")
      .agg(
        F.approx_count_distinct("user_id").alias("old_uv")
      )

    FileUtils.deleteDirectory(new File("d:/uvssss/"))
    users
      .withColumn("event_date", F.to_date($"event_date"))
      .groupBy("event_date")
      .agg(
        F.approx_count_distinct("user_unique_id").alias("all_uv")
      )
      .join(oldUv, "event_date")
      .withColumn("new_uv", $"all_uv" - $"old_uv")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("d:/uvssss/")

    FileUtils.deleteDirectory(new File("d:/lucky_new_user/"))
    FileUtils.deleteDirectory(new File("d:/lucky_old_user/"))

    val oldOrdered = loi
      .withColumn("event_date", F.to_date($"create_time"))
      .groupBy("event_date", "user_tag")
      .agg(
        F.approx_count_distinct("luckystar_order_id").alias("order_num"),
        F.approx_count_distinct("user_id").alias("order_user")
      )

    val oldPayed = loi
      .withColumn("event_date", F.to_date($"pay_time"))
      .groupBy("event_date", "user_tag")
      .agg(
        F.approx_count_distinct("luckystar_order_id").alias("pay_num"),
        F.approx_count_distinct("user_id").alias("pay_user"),
        F.sum($"order_amount" - $"bonus").alias("gmv")
      )


    oldOrdered
      .join(oldPayed, Seq("event_date", "user_tag"), "left")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("d:/lucky_old_user/")


    //表三
    //    val winSpec = Window
    //      .partitionBy("user_id")
    //      .orderBy("create_time")
    //    val oi = spark
    //      .read
    //      .option("header", "true")
    //      .csv("d:/luckystar_winning_record.csv")
    //      .withColumn("first_win_time", F.first("create_time", true).over(winSpec))
    //      .select("user_id", "first_win_time")
    //
    //
    //    val firstWin = oi
    //      .select("user_id", "first_win_time")
    //      .distinct()
    //
    //
    //    val loi = spark
    //      .read
    //      .option("header", "true")
    //      .csv("d:/luckystar_order_info.csv")
    //      .join(firstWin, Seq("user_id"), "left")
    //      .withColumn("user_tag", F.when($"pay_time" >= $"first_win_time", "win").otherwise("not_win"))
    //
    //    val users = spark
    //      .read
    //      .option("header", "true")
    //      .csv("d:/lucky_star_users.csv")
    //      .withColumn("event_date", F.to_date($"event_date"))
    //      .join(loi.select("user_id", "first_win_time"), $"user_id" === $"user_unique_id")
    //      .withColumn("user_tag", F.when($"event_date" >= $"first_win_time", "win").otherwise("not_win"))
    //
    //
    //    FileUtils.deleteDirectory(new File("d:/lucky_new_user/"))
    //    FileUtils.deleteDirectory(new File("d:/lucky_old_user/"))
    //
    //
    //    val oldOrdered = loi
    //      .withColumn("event_date", F.to_date($"create_time"))
    //      .groupBy("event_date", "user_tag")
    //      .agg(
    //        F.approx_count_distinct("luckystar_order_id").alias("order_num"),
    //        F.approx_count_distinct("user_id").alias("order_user")
    //      )
    //
    //    val oldPayed = loi
    //      .withColumn("event_date", F.to_date($"pay_time"))
    //      .groupBy("event_date", "user_tag")
    //      .agg(
    //        F.approx_count_distinct("luckystar_order_id").alias("pay_num"),
    //        F.approx_count_distinct("user_id").alias("pay_user"),
    //        F.sum($"order_amount" - $"bonus").alias("gmv")
    //      )
    //
    //    val oldUv = users
    //      .groupBy("event_date", "user_tag")
    //      .agg(
    //        F.approx_count_distinct("user_id").alias("uv")
    //      )
    //
    //    oldUv.show(200, false)
    //    oldPayed.show(200, false)
    //    oldUv
    //      .join(oldOrdered, Seq("event_date", "user_tag"), "left")
    //      .join(oldPayed, Seq("event_date", "user_tag"), "left")
    //      .coalesce(1)
    //      .write
    //      .option("header", "true")
    //      .mode(SaveMode.Append)
    //      .csv("d:/lucky_old_user/")


    //表4
    //
    //    val loi = spark
    //      .read
    //      .option("header", "true")
    //      .csv("d:/luckystar_order_info.csv")
    //      .filter($"pay_status" >= 1)
    //
    //    val winSpec = Window.partitionBy("user_id").orderBy("pay_time")
    //
    //
    //    val payedLoi = loi
    //      .filter($"pay_status" >= 1)
    //      .withColumn("first_pay_time", F.first("pay_time", true).over(winSpec))
    //      .withColumn("diff", F.datediff($"pay_time", $"first_pay_time"))
    //      .cache()
    //
    //    payedLoi.show(false)
    //    val data2 = payedLoi
    //      .withColumn("diff", F
    //        .when($"diff" === 0, "0")
    //        .when($"diff" < 1, "(0, 1)")
    //        .when($"diff" < 2, "[1, 2)")
    //        .when($"diff" < 3, "[2, 3)")
    //        .when($"diff" < 4, "[3, 4)")
    //        .when($"diff" < 5, "[4, 5)")
    //        .otherwise("[5, )")
    //      )
    //      .withColumn("event_date", F.to_date($"pay_time"))
    //      .groupBy("event_date", "diff")
    //      .agg(
    //        F.approx_count_distinct("user_id").alias("uv")
    //      )
    //
    //    writeToCSV(data2)


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
        |  AND amp.event_type IN (400, 410, 411, 412, 413, 414)
        |  AND amp.push_time >= '2019-06-10'
        |  AND aelmp.event_type = 'click'
        |  AND amp.push_date >= '2019-06-10'
        |  AND aelmp.event_time >= '2019-06-10'
        |  AND aelmp.event_time >= amp.push_time
      """.stripMargin

    val tryPush =
      """
        |SELECT amp.push_time,
        |       amp.user_id,
        |       amp.event_type
        |FROM app_message_push amp
        |WHERE amp.push_result = 2
        |  AND amp.event_type IN (400, 410, 411, 412, 413, 414)
        |  AND amp.push_time >= '2019-06-10'
      """.stripMargin

    val newOrder =
      """
        |SELECT loi.order_amount AS order_gmv,
        |       loi.order_amount - loi.bonus AS grand_total,
        |       loi.create_time  AS order_time,
        |       loi.user_id,
        |       loi.pay_status,
        |       loi.pay_time
        |FROM luckystar_order_info loi
        |WHERE loi.create_time >= '2019-06-10'
      """.stripMargin

    val oldOrder =
      """
        |SELECT oi.order_amount AS order_gmv,
        |       oi.goods_amount + oi.shipping_fee AS grand_total,
        |       oi.order_time,
        |       oi.user_id,
        |       oi.pay_status,
        |       oi.pay_time
        |FROM order_info oi
        |WHERE oi.order_time >= '2019-06-10'
        | and  exists(select * from order_extension oe where oe.order_id = oi.order_id and oe.ext_name = 'luckystar_activity_id')
      """.stripMargin

    val reportDb = new DataSource("themis_report_read")
    //    reportDb
    //      .load(spark, clickPush)
    //      .write
    //      .option("header", "true")
    //      .mode(SaveMode.Append)
    //      .csv("d:/click_push_csv/")
    //
    //    reportDb.load(spark, tryPush)
    //      .write
    //      .option("header", "true")
    //      .mode(SaveMode.Append)
    //      .csv("d:/try_push_csv/")

    val themisDb = new DataSource("themis_read")
    themisDb.load(spark, newOrder)
      .write
      .option("header", "true")
      .mode(SaveMode.Append)
      .csv("d:/order_info_csv/")

    themisDb.load(spark, oldOrder)
      .write
      .option("header", "true")
      .mode(SaveMode.Append)
      .csv("d:/order_info_csv/")

    import spark.implicits._

    spark.read
      .option("header", "true")
      .csv("D:\\click_push_csv\\")
      .withColumn("push_time", F.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .withColumn("event_time", F.regexp_replace($"event_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("click_push")

    spark.read
      .option("header", "true")
      .csv("d:/try_push_csv/")
      .withColumn("push_time", F.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("try_push")


    spark.read
      .option("header", "true")
      .csv("D:\\order_info_csv")
      .withColumn("order_time", F.regexp_replace($"order_time".substr(0, 19), "T", " "))
      .withColumn("pay_time", F.regexp_replace($"pay_time".substr(0, 19), "T", " "))
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
        | sum(order_gmv) as try_pay_gmv,
        | sum(grand_total) as try_grand_total
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
        | sum(order_gmv) as click_pay_gmv,
        | sum(grand_total) as click_grand_total
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

  def d_1450(spark: SparkSession): Unit = {
    spark.read
      .option("header", "true")
      .csv("d:/app_versions.csv")
      .createOrReplaceTempView("av")

    spark.read
      .option("header", "true")
      .csv("d:/notices.csv")
      .createOrReplaceTempView("nt")


    val data = spark.sqlContext.sql(
      """
        |select
        | *
        |from av
        | inner join nt using(device_id)
        |where nt.platform = 'ios'
      """.stripMargin)

    writeToCSV(data)
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
    //println(Conf.getString("s3.etl.primitive.a_b_devices"))
    import spark.implicits._
    d_1334(spark)
    //merge(spark)
    spark.close()

  }


}