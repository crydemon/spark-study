package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.conf.Conf
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

  def dropTable: String =
    """
      |drop table ab_report
    """.stripMargin

  def updateDeviceSql: String =
    """
      |SELECT replace(config -> '$[0].test', '"', '')      AS test_version,
      |       replace(config -> '$[0].test_name', '"', '') AS test_name,
      |       devices                                      AS device_id,
      |       app_version,
      |       abtest_version
      |FROM a_b_test_record
    """.stripMargin

  def main(args: Array[String]): Unit = {

    val reportDb = new DataSource("themis_report_write")
//    reportDb.execute(dropTable)
//    reportDb.execute(createTable)

    val themisDb = new DataSource("themis_read")
    //    reportDb.execute(dropTable)
    //    reportDb.execute(createTable)


    val appName = "brand_report"

    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")


    themisDb
      .load(spark, updateDeviceSql)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(Conf.getString("s3.etl.primitive.a_b_devices"))

    val ABDevices = spark.read
      .parquet(Conf.getString("s3.etl.primitive.a_b_devices"))

    import spark.implicits._
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))

    while (start.compareTo(end) <= 0) {
      val next = start.plusDays(1)
      val startS = start.format(dateFormat)
      val endS = next.format(dateFormat)
      //原数据
      val hit = spark.read
        .json("s3://vomkt-evt/batch-processed/hit/" + startS + "T*")
        //.json("d:/vomkt-evt/batch-processed/hit/" + startS)
        .withColumn("os_family", F.lower($"os_family"))
        .filter($"os_family".isin("android", "ios"))
        .withColumn("device_id", F.coalesce($"device_id", $"organic_idfv", $"android_id", $"idfa", $"idfv"))
        .withColumn("cur_day", F.to_date($"derived_ts"))
        .select("event_name", "element_name", "os_family", "country", "page_code", "url", "device_id", "cur_day")
        .withColumnRenamed("country", "region_code")
        .withColumnRenamed("os_family", "platform")
        .join(ABDevices, "device_id")
        .cache()

      //求gmv
      val orderInfo =
        s"""
          |SELECT oi.order_id,
          |       oi.order_time,
          |       oi.goods_amount + oi.shipping_fee AS gmv,
          |       oi.country                        AS region_code,
          |       CASE
          |           WHEN oi.device_type = 11 THEN 'ios'
          |           WHEN oi.device_type = 12 THEN 'android'
          |           END                           AS platform,
          |       oi.user_id,
          |       oi.pay_status,
          |       oi.device_id,
          |       oi.pay_time
          |FROM order_info oi
          |WHERE oi.order_time >= '$startS'
          |  AND oi.order_time < '$endS'
          |  AND oi.parent_order_id = 0
        """.stripMargin

      val oiRaw = reportDb
        .load(spark, orderInfo)

      //组合每种情况
      for {
        platform <- List(F.col("platform"), F.lit("all"))
        regionCode <- List(F.col("region_code"), F.lit("all"))
      } {
        //商详页
        val productDetail = hit
          .filter($"page_code" === "product_detail")
          .filter($"event_name" === "screen_view")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
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
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("cart")
          )

        //加购成功
        val cartSuccess = hit
          .filter($"element_name" === "pdAddToCartSuccess")
          .filter($"event_name" === "common_click")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("cart_success")
          )

        val placeOrder = hit
          .filter($"element_name" === "button_checkout_placeOrder")
          .filter($"event_name" === "order_process")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("place_order")
          )

        val homepageDau = hit
          .filter($"element_name" === "homepage")
          .filter($"event_name" === "screen_view")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("homepage_dau")
          )

        val orderedDf = oiRaw
          .join(ABDevices, "device_id")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("cur_day", F.to_date($"order_time"))
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("order_user"),
            F.count("order_id").alias("order_num")
          )

        val payedDf = oiRaw
          .filter($"pay_status" >= 1)
          .join(ABDevices, "device_id")
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .withColumn("cur_day", F.to_date($"pay_time"))
          .groupBy("cur_day", "region_code", "platform", "test_name", "test_version", "app_version")
          .agg(
            F.approx_count_distinct("device_id").alias("pay_user"),
            F.count("order_id").alias("pay_num"),
            F.sum("gmv").alias("gmv")
          )

        val finalData = homepageDau
          .join(productDetail, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(cart, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(cartSuccess, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(checkout, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(placeOrder, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(orderedDf,Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
          .join(payedDf, Seq("cur_day","region_code", "platform", "test_name", "test_version", "app_version"), "left")
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
