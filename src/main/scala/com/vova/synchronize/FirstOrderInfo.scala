package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import com.vova.utils.S3Config
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object FirstOrderInfo {

  def createTable: String =
    """
      |CREATE TABLE `first_order_info`
      |(
      |    `device_id`        varchar(64) NOT NULL DEFAULT '',
      |    `user_id`          int(11)     NOT NULL DEFAULT '0',
      |    `idfa`             varchar(64),
      |    `platform`         varchar(8),
      |    `currency`         varchar(8),
      |    `advertising_id`   varchar(64),
      |    `bundle_id`        varchar(64),
      |    `media_source`     varchar(128),
      |    `appsflyer_device_id` varchar(128),
      |    `first_order_id`   bigint               DEFAULT 0,
      |    `first_pay_time`   bigint      NOT NULL DEFAULT 0,
      |    `create_time`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
      |    `last_update_time` timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last update time',
      |    PRIMARY KEY `device_id` (`device_id`),
      |    KEY `user_id` (`user_id`),
      |    KEY `first_order_id` (`first_order_id`),
      |    KEY `first_pay_time` (`first_pay_time`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='首单信息';
    """.stripMargin


  def initTable(spark: SparkSession, start: LocalDate, end: LocalDate): Unit = {

    def getPayedInfo(startTime: String, endTime: String): String = {
      s"""
         |SELECT ar.device_id,
         |       oi.user_id,
         |       su.idfa,
         |       ar.platform,
         |       su.currency,
         |       ar.advertising_id,
         |       su.bundle_id,
         |       oi.order_id,
         |       ar.media_source,
         |       ar.appsflyer_device_id,
         |       unix_timestamp(oi.pay_time) as pay_time
         |FROM appsflyer_record ar
         |         INNER JOIN app_event_log_user_start_up su USING (device_id)
         |         INNER JOIN order_info oi USING (device_id)
         |WHERE ar.install_time >= '$startTime'
         |  AND ar.install_time < '$endTime'
         |  AND oi.pay_status >= 1
         |  AND oi.parent_order_id = 0
    """.stripMargin
    }

    import spark.implicits._
    val reportDb = new DataSource("themis_report_read")
    val reportDbWriter = new DataSource("themis_report_write")
    //    reportDbWriter.execute(dropTable)
    ////    println("drop table success")
    ////    reportDbWriter.execute(createTable)
    ////    println("create table success")
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    var curDay = start
    while (curDay.compareTo(end) <= 0) {
      val nextDay = curDay.plusDays(1)
      val startTime = curDay.format(dateFormat) + "-00:00:00"
      val endTime = nextDay.format(dateFormat) + "-00:00:00"
      val orderInfo = reportDb.load(spark, getPayedInfo(startTime, endTime))

      println(startTime)

      val rankSpec = Window.partitionBy("device_id").orderBy(orderInfo("order_id"))
      val orderInfoRank = orderInfo
        .withColumn("rank", dense_rank().over(rankSpec))
        .filter($"rank" === 1)
        .withColumn("first_order_id", coalesce($"order_id", lit(0)))
        .withColumn("first_pay_time", coalesce($"pay_time", lit(0)))
        .select("device_id", "user_id", "idfa",
          "platform", "currency", "advertising_id", "bundle_id", "first_order_id", "first_pay_time", "media_source", "appsflyer_device_id")
        .cache()

      //orderInfoRank.show(truncate = false)
      reportDbWriter.insertPure("first_order_info", orderInfoRank, spark)
      curDay = nextDay
    }
  }

  def updateTable(spark: SparkSession, start: LocalDate, end: LocalDate): Unit = {

    def getPayedInfo(startTime: String, endTime: String): String = {
      s"""
         |SELECT ar.device_id,
         |       oi.user_id,
         |       su.idfa,
         |       ar.platform,
         |       su.currency,
         |       ar.advertising_id,
         |       su.bundle_id,
         |       oi.order_id,
         |       unix_timestamp(oi.pay_time) as pay_time,
         |       ar.media_source,
         |       ar.appsflyer_device_id,
         |       foi.first_order_id,
         |       foi.first_pay_time
         |FROM appsflyer_record ar
         |         INNER JOIN app_event_log_user_start_up su USING (device_id)
         |         INNER JOIN order_info oi USING (device_id)
         |         LEFT  JOIN first_order_info foi USING(device_id)
         |WHERE oi.pay_time >= '$startTime'
         |  AND oi.pay_time < '$endTime'
         |  AND oi.pay_status >= 1
         |  AND oi.parent_order_id = 0
    """.stripMargin
    }

    import spark.implicits._
    val reportDb = new DataSource("themis_report_read")
    val reportDbWriter = new DataSource("themis_report_write")
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    var curDay = start
    while (curDay.compareTo(end) <= 0) {
      val nextDay = curDay.plusDays(1)
      val startTime = curDay.format(dateFormat) + "-00:00:00"
      val endTime = nextDay.format(dateFormat) + "-00:00:00"
      val orderInfo = reportDb.load(spark, getPayedInfo(startTime, endTime))

      val rankSpec = Window.partitionBy("device_id").orderBy(orderInfo("order_id"))
      val orderInfoRank = orderInfo
        .withColumn("rank", dense_rank().over(rankSpec))
        .filter($"rank" === 1)
        .withColumn("first_order_id", coalesce($"first_order_id", $"order_id", lit(0)))
        .withColumn("first_pay_time", coalesce($"first_pay_time", $"pay_time", lit(0)))
        .select("device_id", "user_id", "idfa",
          "platform", "currency", "advertising_id", "bundle_id", "first_order_id", "first_pay_time", "media_source", "appsflyer_device_id")
        .cache()

      //orderInfoRank.show(truncate = false)
      reportDbWriter.insertPure("first_order_info", orderInfoRank, spark)
      curDay = nextDay
    }


  }

  def upload(spark: SparkSession): Unit = {
    def getPayedInfo: String = {
      s"""
         |SELECT ut.device_id,
         |       0 AS is_new_user
         |FROM user_tags ut
         |WHERE ut.acc_payed_order > 0
    """.stripMargin
    }

    def getGoodsBrand: String = {
     """
       |SELECT CASE
       |           WHEN c.depth = 1
       |               THEN c.cat_name
       |           WHEN c_pri.depth = 1
       |               THEN c_pri.cat_name
       |           WHEN c_ga.depth = 1
       |               THEN c_ga.cat_name
       |           END AS first_class,
       |       CASE
       |           WHEN c.depth = 2
       |               THEN c.cat_name
       |           WHEN c_pri.depth = 2
       |               THEN c_pri.cat_name
       |           WHEN c_ga.depth = 2
       |               THEN c_ga.cat_name
       |           END AS second_class,
       |       g.goods_id,
       |       vg.virtual_goods_id,
       |       brand_id
       |FROM goods g
       |         INNER JOIN virtual_goods vg USING (goods_id)
       |         LEFT JOIN category c ON g.cat_id = c.cat_id
       |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
       |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
     """.stripMargin
    }

    val reportDb = new DataSource("themis_report_read")
    reportDb.load(spark, getPayedInfo)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://vomkt-emr-rec/testresult/first_order_info/")
    val themisDb = new DataSource("themis_read")
    themisDb.load(spark, getGoodsBrand)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("s3://vomkt-emr-rec/testresult/goods/")
  }

  def main(args: Array[String]): Unit = {

    val appName = "first_order_info"
    println(appName)
    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")

    //    val spark = SparkSession.builder
    //      .appName(appName)
    //      .master("local[*]")
    //      .getOrCreate()
    //    spark.sparkContext.setLogLevel("WARN")
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))
    //initTable(spark, start, end)
    updateTable(spark, start, end)
    upload(spark)
    spark.stop()
  }
}
