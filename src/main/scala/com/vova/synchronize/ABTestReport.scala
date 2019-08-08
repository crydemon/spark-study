package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions => F}

object ABTestReport {

  def createTable: String =
    """
      |CREATE TABLE `ab_report`
      |(
      |    `id`               bigint AUTO_INCREMENT,
      |    `cur_day`          timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
      |    `region_code`      varchar(4)         DEFAULT '',
      |    `platform`         varchar(8)         DEFAULT '',
      |    `test_name`        varchar(32)        DEFAULT '',
      |    `test_version`     varchar(16)        DEFAULT '',
      |    `app_version`      varchar(16)        DEFAULT '',
      |    `product_detail`   int                DEFAULT 0 COMMENT '商详页',
      |    `cart`             int                DEFAULT 0 COMMENT '加购数',
      |    `cart_success`     int                DEFAULT 0 COMMENT '加购成功数',
      |    `place_order`      int                DEFAULT 0,
      |    `checkout`         int                DEFAULT 0,
      |    `order_user`       int                DEFAULT 0,
      |    `order_num`        int                DEFAULT 0,
      |    `pay_user`         int                DEFAULT 0,
      |    `pay_num`          int                DEFAULT 0,
      |    `gmv`              decimal(10, 2)     DEFAULT 0.00,
      |    `create_time`      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
      |    `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last update time',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`cur_day`, `platform`, `region_code`, `test_name`, `test_version`, `app_version`),
      |    KEY `test_name` (`test_name`),
      |    KEY `region_code` (`region_code`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='ab报表';
    """.stripMargin


  def createABDevicesTable: String = {
    """
      |CREATE TABLE `ab_devices`
      |(
      |    `id`               bigint AUTO_INCREMENT,
      |    `platform`         varchar(8)         DEFAULT '',
      |    `test_name`        varchar(64)        DEFAULT '',
      |    `test_version`     varchar(16)        DEFAULT '',
      |    `device_id`        varchar(64)        DEFAULT '',
      |    `app_version`      varchar(16)        DEFAULT '',
      |    `create_time`      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
      |    `expire_time`      timestamp NOT NULL DEFAULT '2022-03-01 00:00:00' COMMENT '过期则删除设备记录',
      |    `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last update time',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`device_id`, `platform`, `test_name`, `test_version`, `app_version`),
      |    KEY `test_name` (`test_name`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='ab devices'
    """.stripMargin
  }

  def dropTable: String =
    """
      |drop table ab_report
    """.stripMargin

  def dropABDevicesTable: String =
    """
      |drop table ab_devices
    """.stripMargin


  def main(args: Array[String]): Unit = {

    val reportDb = new DataSource("themis_report_write")
    val sql = "DELETE FROM ab_devices WHERE test_version = ''"
    reportDb.execute(sql)
    //    reportDb.execute(dropABDevicesTable)
    //    reportDb.execute(createABDevicesTable)
    //    reportDb.execute(dropTable)
    //    reportDb.execute(createTable)


    val appName = "ab_report"
    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")


    import spark.implicits._
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))

    val ABDevicesSql =
      """
        |select  device_id, test_name, test_version, app_version
        |from ab_devices
      """.stripMargin

    val ABDevicesInMysql = reportDb.load(spark, ABDevicesSql)

    val needCollectTestNames = ("new_ProductDetail5.0")

    while (start.compareTo(end) <= 0) {
      val next = start.plusDays(1)
      val startS = start.format(dateFormat)
      val endS = next.format(dateFormat)
      //原数据
      val rawData = spark.read
        .json("s3://vomkt-evt/batch-processed/hit/" + startS + "T*")
        .filter(F.lower($"os").isin("android", "ios"))
        .withColumn("device_id", F.coalesce($"device_id", $"organic_idfv", $"android_id", $"idfa", $"idfv"))
        .withColumn("cur_day", F.to_date($"derived_ts"))

      val rawABDevices = rawData
        .filter($"test_name".isNotNull)
        .filter($"device_id".isNotNull)
        .filter($"test_version".isNotNull and ($"test_version" =!= ""))
        .filter($"event_name" === "test_create")
        .filter($"test_name".isin(needCollectTestNames))
        .select("device_id", "os", "test_name", "test_version", "app_version")
        .filter($"os" === "android" && $"app_version" =!= "2.34.0")
        .withColumnRenamed("os", "platform")
        .distinct()

      reportDb.insertPure("ab_devices", rawABDevices, spark)

      val ABDevices = ABDevicesInMysql
        .union(rawABDevices
          .select("device_id", "test_name", "test_version", "app_version")
        )
        .distinct()


      val hit = rawData
        .filter($"os".isin("android", "ios"))
        .where("page_code not in('h5flashsale', 'luckystar', 'auction_auctionhouse')")
        .select("event_name", "element_name", "country", "os", "page_code", "url", "device_id", "cur_day")
        .withColumnRenamed("country", "region_code")
        .withColumnRenamed("os", "platform")
        .join(ABDevices, "device_id")
        .cache()


      //求gmv
      val orderInfo =
        s"""
           |SELECT oi.order_id,
           |       oi.order_time,
           |       oi.goods_amount + oi.shipping_fee AS gmv,
           |       r.region_code                     AS region_code,
           |       CASE
           |           WHEN oi.device_type = 11 THEN 'ios'
           |           WHEN oi.device_type = 12 THEN 'android'
           |           END                           AS platform,
           |       oi.user_id,
           |       oi.pay_status,
           |       oi.device_id,
           |       oi.pay_time
           |FROM order_info oi
           |         INNER JOIN region r ON r.region_id = oi.country
           |WHERE oi.order_time >= '$startS'
           |  AND oi.order_time < '$endS'
           |  AND oi.parent_order_id = 0
         """.stripMargin

      val oiRaw = reportDb
        .load(spark, orderInfo)
        .join(ABDevices, "device_id")

      //组合每种情况
      for {
        platform <- List(F.col("platform"), F.lit("all"))
        regionCode <- List(F.col("region_code"), F.lit("all"))
        appVersion <- List(F.col("app_version"), F.lit("all"))
      } {
        //商详页
        val productDetail = hit
          .filter($"page_code" === "product_detail")
          .filter($"event_name" === "screen_view")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("product_detail")
          )


        //checkout
        val checkout = hit
          .filter($"page_code" === "checkout")
          .filter($"event_name" === "screen_view")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("checkout")
          )

        //加购
        val cart = hit
          .filter($"event_name" === "screen_view")
          .filter($"page_code" === "cart")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("cart")
          )

        cart.show(false)

        //加购成功
        val cartSuccess = hit
          .filter($"element_name" === "pdAddToCartSuccess")
          .filter($"event_name" === "common_click")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("cart_success")
          )

        val placeOrder = hit
          .filter($"element_name" === "button_checkout_placeOrder")
          .filter($"event_name" === "order_process")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("place_order")
          )

        val homepageDau = hit
          .filter($"page_code" === "homepage")
          .filter($"event_name" === "screen_view")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("homepage_dau")
          )

        val orderedDf = oiRaw
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("cur_day", F.to_date($"order_time"))
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("order_user"),
            F.count("order_id").alias("order_num")
          )

        val payedDf = oiRaw
          .filter($"pay_status" >= 1)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("cur_day", F.to_date($"pay_time"))
          .withColumn("app_version", appVersion)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("pay_user"),
            F.count("order_id").alias("pay_num"),
            F.sum("gmv").alias("gmv")
          )


        val finalData = homepageDau
          .join(productDetail, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(cart, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(cartSuccess, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(checkout, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(placeOrder, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(orderedDf, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(payedDf, Seq("cur_day", "region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .select(
            F.coalesce($"cur_day", F.lit("0000-00-00")).alias("cur_day"),
            F.coalesce($"region_code", F.lit("")).alias("region_code"),
            F.coalesce($"platform", F.lit("")).alias("platform"),
            F.coalesce($"test_name", F.lit("")).alias("test_name"),
            F.coalesce($"test_version", F.lit("")).alias("test_version"),
            F.coalesce($"app_version", F.lit("")).alias("app_version"),
            F.coalesce($"product_detail", F.lit(0)).alias("product_detail"),
            F.coalesce($"cart", F.lit(0)).alias("cart"),
            F.coalesce($"cart_success", F.lit(0)).alias("cart_success"),
            F.coalesce($"place_order", F.lit(0)).alias("place_order"),
            F.coalesce($"checkout", F.lit(0)).alias("checkout"),
            F.coalesce($"order_num", F.lit(0)).alias("order_num"),
            F.coalesce($"order_user", F.lit(0)).alias("order_user"),
            F.coalesce($"pay_user", F.lit(0)).alias("pay_user"),
            F.coalesce($"pay_num", F.lit(0)).alias("pay_num"),
            F.coalesce($"gmv", F.lit(0.00)).alias("gmv")
          )
          .cache()

        reportDb.insertPure("ab_report", finalData, spark)
      }
      start = next
    }

  }

}
