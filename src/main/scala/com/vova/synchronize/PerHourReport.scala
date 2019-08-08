package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.conf.Conf
import com.vova.db.DataSource
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object PerHourReport {
  val themisDb = new DataSource("themis_read")

  def createTable: String = {
    """
      |CREATE TABLE per_hour_report
      |(
      |    `id`            bigint AUTO_INCREMENT,
      |    `activity_name` varchar(32) NOT NULL DEFAULT '',
      |    `field_name`    varchar(48) NOT NULL DEFAULT '',
      |    `platform`      varchar(48) NOT NULL DEFAULT '',
      |    `cur_day`       date        NOT NULL DEFAULT '0000-00-00',
      |    `0h`            int         NOT NULL DEFAULT '0',
      |    `1h`            int         NOT NULL DEFAULT '0',
      |    `2h`            int         NOT NULL DEFAULT '0',
      |    `3h`            int         NOT NULL DEFAULT '0',
      |    `4h`            int         NOT NULL DEFAULT '0',
      |    `5h`            int         NOT NULL DEFAULT '0',
      |    `6h`            int         NOT NULL DEFAULT '0',
      |    `7h`            int         NOT NULL DEFAULT '0',
      |    `8h`            int         NOT NULL DEFAULT '0',
      |    `9h`            int         NOT NULL DEFAULT '0',
      |    `10h`           int         NOT NULL DEFAULT '0',
      |    `11h`           int         NOT NULL DEFAULT '0',
      |    `12h`           int         NOT NULL DEFAULT '0',
      |    `13h`           int         NOT NULL DEFAULT '0',
      |    `14h`           int         NOT NULL DEFAULT '0',
      |    `15h`           int         NOT NULL DEFAULT '0',
      |    `16h`           int         NOT NULL DEFAULT '0',
      |    `17h`           int         NOT NULL DEFAULT '0',
      |    `18h`           int         NOT NULL DEFAULT '0',
      |    `19h`           int         NOT NULL DEFAULT '0',
      |    `20h`           int         NOT NULL DEFAULT '0',
      |    `21h`           int         NOT NULL DEFAULT '0',
      |    `22h`           int         NOT NULL DEFAULT '0',
      |    `23h`           int         NOT NULL DEFAULT '0',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`activity_name`, `field_name`, `platform`, `cur_day`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='分小时报表';
    """.stripMargin
  }

  def dropTable: String = {
    """
      |drop table per_hour_report
    """.stripMargin
  }

  def initSpark: SparkSession = {
    val appName = "recall_pool"
    System.getenv("BUILD_ENV") match {
      case "local" => {
        println(appName)
        val spark = SparkSession.builder
          .appName("recall_pool")
          .master("local[*]")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark
      }
      case _ => {
        val spark = SparkSession.builder
          .master("yarn")
          .appName(appName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.yarn.maxAppAttempts", 1)
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")
        spark
      }
    }
  }

  def loadBatchProcessed(path: String, spark: SparkSession): DataFrame = {
    println(Conf.getString(" s3.evt.batch.root") + path + "T*")
    spark.read
      .json(Conf.getString(" s3.evt.batch.root") + path + "T*")
  }


  def auctionTransform(path: String, spark: SparkSession): Unit = {
    def getJoinTask(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(aaml.create_time)               AS cur_day,
           |       hour(aaml.create_time)               AS cur_hour,
           |       air.platform,
           |       count(DISTINCT aaml.auction_user_id) AS join_auction
           |FROM activity_auction_mission_log aaml
           |         LEFT JOIN app_install_record air ON air.user_id = aaml.auction_user_id
           |WHERE aaml.create_time >= '$startTime'
           |  AND aaml.create_time < '$endTime'
           |  AND aaml.auction_user_id > 0
           |GROUP BY cur_day, cur_hour, platform
      """.stripMargin
      themisDb.load(spark, sql)
    }

    def getAuctionSuccess(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(aam.create_time)                 AS cur_day,
           |       hour(aam.create_time)                 AS cur_hour,
           |       air.platform,
           |       count(1)                              AS ac_success_pv,
           |       count(DISTINCT aam.top_price_user_id) AS ac_success_uv
           |FROM activity_auction_mission aam
           |         LEFT JOIN app_install_record air ON air.user_id = aam.top_price_user_id
           |WHERE aam.create_time >= '$startTime'
           |  AND aam.create_time < '$endTime'
           |  AND aam.top_price_user_id > 0
           |  AND aam.status IN (1, 2, 21, 22, 3)
           |GROUP BY cur_day, cur_hour, platform
       """.stripMargin
      themisDb.load(spark, sql)
    }

    def getOrdered(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(oi.order_time)         AS cur_day,
           |       CASE
           |           WHEN ore.device_type = 11 THEN 'ios'
           |           WHEN ore.device_type = 12 THEN 'android'
           |       END                     AS platform,
           |       hour(oi.order_time)         AS cur_hour,
           |       count(DISTINCT oi.order_id) AS order_num
           |FROM activity_auction_mission aam
           |         INNER JOIN order_info oi USING (order_id)
           |         INNER JOIN order_relation ore USING (order_sn)
           |WHERE oi.order_time >= '$startTime'
           |  AND oi.order_time < '$endTime'
           |  AND aam.top_price_user_id > 0
           |GROUP BY cur_day, cur_hour, platform
       """.stripMargin
      themisDb.load(spark, sql)
    }

    def getPayed(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(oi.pay_time)                      AS cur_day,
           |       CASE
           |           WHEN ore.device_type = 11 THEN 'ios'
           |           WHEN ore.device_type = 12 THEN 'android'
           |           END                                AS platform,
           |       hour(oi.order_time)                    AS cur_hour,
           |       count(DISTINCT oi.order_id)            AS pay_num,
           |       sum(oi.shipping_fee + oi.goods_amount) AS gmv
           |FROM activity_auction_mission aam
           |         INNER JOIN order_info oi USING (order_id)
           |         INNER JOIN order_relation ore USING (order_sn)
           |WHERE oi.pay_time >= '$startTime'
           |  AND oi.pay_time < '$endTime'
           |  AND oi.pay_status >= 1
           |  AND aam.top_price_user_id > 0
           |GROUP BY cur_day, cur_hour, platform
      """.stripMargin
      themisDb.load(spark, sql)
    }
    import spark.implicits._

    val reportDb = new DataSource("themis_report_write")
    val filters = List(
      ("拍卖场主会场曝光", "page_code='auction_auctionhouse' and event_name = 'page_view'")
    )
    val layer_0 = loadBatchProcessed("hit/" + path, spark)
      .withColumn("event_time", F.to_utc_timestamp($"derived_ts", "yyyy-MM-dd HH:mm:ss"))

    for {
      platform <- List(F.lit("all"), F.coalesce($"os", F.lower($"os_family")))
      filter <- filters
    } {
      val layer_1 = layer_0
        .where(filter._2)
        .withColumn("cur_day", F.to_date($"event_time"))
        .withColumn("cur_hour", F.concat(F.hour($"event_time"), F.lit("h")))
        .withColumn("platform", platform)
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.approx_count_distinct("domain_userid").alias("uv")
        )
        .withColumn("field_name", F.lit(filter._1))
        .withColumn("activity_name", F.lit("auction"))

      reportDb.insertPure("per_hour_report", layer_1, spark)
      layer_1.show(false)
    }
    val start = path.replace('/', '-')
    val end = path.replace('/', '-') + " 23:59:59"
    //all df
    val joinTask = getJoinTask(start, end, spark).cache()
    val auctionSuccess = getAuctionSuccess(start, end, spark).cache()
    val ordered = getOrdered(start, end, spark).cache()
    val payed = getPayed(start, end, spark).cache()
    for {
      platform <- List(F.lit("all"), F.col("platform"))
    } {
      val joinTask_1 = joinTask
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("join_auction").alias("join_auction")
        )
        .withColumn("field_name", F.lit("参与竞拍UV"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0)

      joinTask_1.show(false)
      reportDb.insertPure("per_hour_report", joinTask_1, spark)
      val auctionSuccess_1 = auctionSuccess
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("ac_success_uv").alias("ac_success_uv")
        )
        .withColumn("field_name", F.lit("竞拍成功UV"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0)

      reportDb.insertPure("per_hour_report", auctionSuccess_1, spark)

      val auctionSuccess_2 = auctionSuccess
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("ac_success_pv").alias("ac_success_pv")
        )
        .withColumn("field_name", F.lit("竞拍成功PV"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", auctionSuccess_2, spark)

      val ordered_1 = ordered
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("order_num").alias("order_num")
        )
        .withColumn("field_name", F.lit("拍卖订单下单数"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", ordered_1, spark)

      val payed_1 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("pay_num").alias("pay_num")
        )
        .withColumn("field_name", F.lit("拍卖订单支付成功数"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", payed_1, spark)

      val payed_2 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("gmv").alias("gmv")
        )
        .withColumn("field_name", F.lit("gmv"))
        .withColumn("activity_name", F.lit("auction"))
        .na.fill(0.00)

      reportDb.insertPure("per_hour_report", payed_2, spark)
    }

  }

  def luckyStarTransform(path: String, spark: SparkSession): Unit = {

    def getPayed(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(pay_time)                                                                        AS cur_day,
           |       hour(pay_time)                                                                        AS cur_hour,
           |       (SELECT platform FROM app_install_record air WHERE air.user_id = loi.user_id LIMIT 1) AS platform,
           |       count(1)                                                                              AS pay_num,
           |       sum(loi.order_amount)                                                                 AS gmv
           |FROM luckystar_order_info loi
           |         INNER JOIN users u USING (user_id)
           |WHERE pay_time >= '$startTime'
           |  AND pay_time <= '$endTime'
           |  AND pay_status >= 1
           |GROUP BY cur_day, cur_hour, platform
       """.stripMargin
      themisDb.load(spark, sql)
    }

    import spark.implicits._

    val reportDb = new DataSource("themis_report_write")
    val filters = List(
      ("一元夺宝页面uv", "page_code='lucky_star' and event_name = 'page_view'")
    )
    val layer_0 = loadBatchProcessed("hit/" + path, spark)
      .withColumn("event_time", F.to_utc_timestamp($"derived_ts", "yyyy-MM-dd HH:mm:ss"))

    for {
      platform <- List(F.lit("all"), F.coalesce($"os", F.lower($"os_family")))
      filter <- filters
    } {
      val layer_1 = layer_0
        .where(filter._2)
        .withColumn("cur_day", F.to_date($"event_time"))
        .withColumn("cur_hour", F.concat(F.hour($"event_time"), F.lit("h")))
        .withColumn("platform", platform)
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.approx_count_distinct("domain_userid").alias("uv")
        )
        .withColumn("field_name", F.lit(filter._1))
        .withColumn("activity_name", F.lit("lucky_star"))

      reportDb.insertPure("per_hour_report", layer_1, spark)
      layer_1.show(false)
    }
    val start = path.replace('/', '-')
    val end = path.replace('/', '-') + " 23:59:59"
    //all df

    val payed = getPayed(start, end, spark).cache()
    for {
      platform <- List(F.lit("all"), F.col("platform"))
    } {
      val payed_1 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("pay_num").alias("pay_num")
        )
        .withColumn("field_name", F.lit("支付成功数"))
        .withColumn("activity_name", F.lit("lucky_star"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", payed_1, spark)

      val payed_2 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("gmv").alias("gmv")
        )
        .withColumn("field_name", F.lit("gmv"))
        .withColumn("activity_name", F.lit("lucky_star"))
        .na.fill(0.00)

      reportDb.insertPure("per_hour_report", payed_2, spark)
    }

  }


  def flashSaleTransform(path: String, spark: SparkSession): Unit = {
    def getOrdered(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(oi.order_time)        AS cur_day,
           |       hour(oi.order_time)        AS cur_hour,
           |       (SELECT CASE
           |                   WHEN ore.device_type = 11 THEN 'ios'
           |                   WHEN ore.device_type = 12 THEN 'android'
           |                   END
           |        FROM order_relation ore
           |        WHERE ore.order_id = oi.order_id
           |        LIMIT 1)                  AS platform,
           |       count(DISTINCT oi.order_id) AS order_num
           |FROM order_info oi
           |         INNER JOIN order_goods og USING (order_id)
           |         INNER JOIN order_goods_extension oge USING (rec_id, order_id)
           |WHERE oi.order_time >= '$startTime'
           |  AND oi.order_time <= '$endTime'
           |  AND oge.ext_name = 'is_flash_sale'
           |GROUP BY cur_day, cur_hour, platform
        """.stripMargin
      themisDb.load(spark, sql)
    }

    def getPayed(startTime: String, endTime: String, spark: SparkSession): DataFrame = {
      val sql =
        s"""
           |SELECT date(oi.pay_time)                           AS cur_day,
           |       hour(oi.pay_time)                           AS cur_hour,
           |       (SELECT CASE
           |                   WHEN ore.device_type = 11 THEN 'ios'
           |                   WHEN ore.device_type = 12 THEN 'android'
           |                   END
           |        FROM order_relation ore
           |        WHERE ore.order_id = oi.order_id
           |        LIMIT 1)                                   AS platform,
           |       sum(og.shipping_fee + og.shop_price_amount) AS gmv,
           |       count(DISTINCT oi.order_id)                 AS pay_num
           |FROM order_info oi
           |         INNER JOIN order_goods og USING (order_id)
           |         INNER JOIN order_goods_extension oge USING (rec_id, order_id)
           |WHERE oi.pay_time >= '$startTime'
           |  AND oi.pay_time <= '$endTime'
           |  AND oi.pay_status >= 1
           |  AND oge.ext_name = 'is_flash_sale'
           |GROUP BY cur_day, cur_hour, cur_hour, platform
       """.stripMargin
      themisDb.load(spark, sql)
    }
    import spark.implicits._

    val reportDb = new DataSource("themis_report_write")
    val filters = List(
      ("主会场曝光UV", "page_code='h5flashsale' and event_name = 'common_click' and element_type = 'onsale'")
    )
    val layer_0 = loadBatchProcessed("hit/" + path, spark)
      .withColumn("event_time", F.to_utc_timestamp($"derived_ts", "yyyy-MM-dd HH:mm:ss"))

    for {
      platform <- List(F.lit("all"), F.coalesce($"os", F.lower($"os_family")))
      filter <- filters
    } {
      val layer_1 = layer_0
        .where(filter._2)
        .withColumn("cur_day", F.to_date($"event_time"))
        .withColumn("cur_hour", F.concat(F.hour($"event_time"), F.lit("h")))
        .withColumn("platform", platform)
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.approx_count_distinct("domain_userid").alias("uv")
        )
        .withColumn("field_name", F.lit(filter._1))
        .withColumn("activity_name", F.lit("flash_sale"))

      reportDb.insertPure("per_hour_report", layer_1, spark)
      layer_1.show(false)
    }
    val start = path.replace('/', '-')
    val end = path.replace('/', '-') + " 23:59:59"
    //all df
    val ordered = getOrdered(start, end, spark).cache()
    val payed = getPayed(start, end, spark).cache()
    for {
      platform <- List(F.lit("all"), F.col("platform"))
    } {
      val ordered_1 = ordered
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("order_num").alias("order_num")
        )
        .withColumn("field_name", F.lit("下单数"))
        .withColumn("activity_name", F.lit("flash_sale"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", ordered_1, spark)

      val payed_1 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("pay_num").alias("pay_num")
        )
        .withColumn("field_name", F.lit("支付成功数"))
        .withColumn("activity_name", F.lit("flash_sale"))
        .na.fill(0)
      reportDb.insertPure("per_hour_report", payed_1, spark)

      val payed_2 = payed
        .withColumn("platform", platform)
        .filter($"platform".isNotNull)
        .withColumn("cur_hour", F.concat($"cur_hour", F.lit("h")))
        .groupBy("cur_day", "platform")
        .pivot("cur_hour")
        .agg(
          F.sum("gmv").alias("gmv")
        )
        .withColumn("field_name", F.lit("gmv"))
        .withColumn("activity_name", F.lit("flash_sale"))
        .na.fill(0.00)

      reportDb.insertPure("per_hour_report", payed_2, spark)
    }

  }

  def main(args: Array[String]): Unit = {
    val reportDb = new DataSource("themis_report_write")
    reportDb.execute(dropTable)
    reportDb.execute(createTable)
    val spark = initSpark
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))
    auctionTransform(start.format(dateFormat), spark)
    luckyStarTransform(start.format(dateFormat), spark)
    flashSaleTransform(start.format(dateFormat), spark)
  }
}
