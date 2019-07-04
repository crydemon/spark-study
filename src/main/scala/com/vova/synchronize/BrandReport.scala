package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions => F}

object BrandReport {

  def createTable: String =
    """
      |CREATE TABLE `brand_report`
      |(
      |    `id`               bigint AUTO_INCREMENT,
      |    `cur_day`          timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
      |    `region_code`     varchar(4)          DEFAULT '',
      |    `platform`         varchar(8)         DEFAULT '',
      |    `first_class`      varchar(64)        DEFAULT '',
      |    `is_brand`         varchar(8)         DEFAULT '',
      |    `pay_user`         int                DEFAULT 0,
      |    `pay_num`          int                DEFAULT 0,
      |    `gmv`              decimal(10, 2)     DEFAULT 0.00,
      |    `dau`              int                DEFAULT 0,
      |    `sum_impressions`  int                DEFAULT 0,
      |    `sum_clicks`       int                DEFAULT 0,
      |    `product_detail`   int                DEFAULT 0 COMMENT '商详页',
      |    `cart`             int                DEFAULT 0 COMMENT '加购数',
      |    `cart_success`     int                DEFAULT 0 COMMENT '加购成功数',
      |    `create_time`      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
      |    `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last update time',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`cur_day`, `first_class`, `is_brand`, `platform`, `region_code`),
      |    KEY `first_class` (`first_class`),
      |    KEY `is_brand` (`is_brand`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='brand统计';
    """.stripMargin

  def dropTable: String =
    """
      |drop table brand_report
    """.stripMargin

  def main(args: Array[String]): Unit = {

    val reportDb = new DataSource("themis_report_write")
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

    import spark.implicits._
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))

    val goodsInfo = spark.read
      .parquet("s3://vomkt-emr-rec/testresult/goods/")
      //.parquet("d:/goods/")
      .select("virtual_goods_id", "first_class", "brand_id")
      .cache()

    while (start.compareTo(end) <= 0) {
      val next = start.plusDays(1)
      val startS = start.format(dateFormat)
      val endS = next.format(dateFormat)
      //原数据
      val ctr = spark.read
        .json("s3://vomkt-evt/batch-processed/goods_ctr_v2/" + startS + "T*")
        //.json("d:/vomkt-evt/batch-processed/goods_ctr_v2/" + startS)
        .filter($"os_type".isin("android", "ios"))
        .select("goods_id", "derived_ts", "clicks", "impressions", "country", "os_type", "domain_userid")
        .withColumnRenamed("country", "region_code")
        .withColumnRenamed("os_type", "platform")
        .cache()

      val hit = spark.read
        .json("s3://vomkt-evt/batch-processed/hit/" + startS + "T*")
        //.json("d:/vomkt-evt/batch-processed/hit/" + startS)
        .withColumn("os_family", F.lower($"os_family"))
        .filter($"os_family".isin("android", "ios"))
        .select("element_id", "domain_userid", "event_name", "element_name", "derived_ts", "os_family", "country", "page_code", "url")
        .withColumnRenamed("country", "region_code")
        .withColumnRenamed("os_family", "platform")
        .cache()

      val gmv =
        s"""
           |SELECT date(oi.pay_time)                      AS cur_day,
           |       r.region_code,
           |       CASE
           |           WHEN c.depth = 1
           |               THEN c.cat_name
           |           WHEN c_pri.depth = 1
           |               THEN c_pri.cat_name
           |           WHEN c_ga.depth = 1
           |               THEN c_ga.cat_name
           |           END                                AS first_class,
           |       brand_id,
           |       CASE
           |           WHEN ore.device_type = 11 THEN 'ios'
           |           WHEN ore.device_type = 12 THEN 'android'
           |           END                                AS platform,
           |       oi.user_id                             AS pay_user,
           |       oi.order_id                            AS pay_num,
           |       og.shipping_fee + og.shop_price_amount AS gmv
           |FROM order_info oi
           |         INNER JOIN region r ON r.region_id = oi.country
           |         INNER JOIN order_relation ore USING (order_id)
           |         INNER JOIN order_goods og USING (order_id)
           |         INNER JOIN goods g USING (goods_id)
           |         LEFT JOIN category c ON g.cat_id = c.cat_id
           |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
           |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
           |WHERE oi.pay_time >= '$startS'
           |  AND oi.pay_time < '$endS'
           |  AND oi.parent_order_id = 0
           |  AND oi.pay_status >= 1
           |  AND oi.from_domain LIKE 'api%'
           |  AND ore.device_type IN (11, 12)
          """.stripMargin

      val gmvDf = themisDb
        .load(spark, gmv)
        .cache()

      //求dau
      val dau =
        s"""
           |SELECT tcdcd.action_date AS cur_day,
           |       platform,
           |       CASE
           |           WHEN country = 'UK' THEN 'GB'
           |           ELSE country
           |           END           AS region_code,
           |       count(1)          AS dau
           |FROM temp_country_device_cohort_details tcdcd
           |WHERE tcdcd.action_date = '$startS'
           |GROUP BY cur_day, platform, region_code
        """.stripMargin
      val dauDf = reportDb
        .load(spark, dau)
      //组合每种情况
      for {
        platform <- List(F.col("platform"), F.lit("all"))
        regionCode <- List(F.col("region_code"), F.lit("all"))
        brand <- List(F.when($"brand_id" > 0, "Y").when($"brand_id" === 0, "N").otherwise("Unknown"), F.lit("all"))
        firstClass <- List(F.col("first_class"), F.lit("all"))
      } {
        //商详页
        val productDetail = hit
          .filter($"page_code" === "product_detail")
          .filter($"event_name" === "screen_view")
          .withColumn("goods_id", F.regexp_extract($"url", "goods_id=([0-9]+)", 1))

        //加购成功
        val cartSuccess = hit
          .filter($"element_name" === "pdAddToCartSuccess")
          .filter($"event_name" === "common_click")

        //加购
        val cart = hit
          .filter($"event_name" === "common_click")
          .filter($"element_name" === "pdAddToCartClick")

        //ctr事件
        val ctr1 = ctr
          .join(goodsInfo, $"goods_id" === $"virtual_goods_id", "left")
          .withColumn("cur_day", F.to_date($"derived_ts"))
          .withColumn("is_brand", brand)
          .withColumn("first_class", firstClass)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "is_brand", "first_class", "region_code", "platform")
          .agg(
            F.sum("impressions").alias("sum_impressions"),
            F.sum("clicks").alias("sum_clicks")
          )
          .cache()


        //商详页
        val productDetail1 = productDetail
          .join(goodsInfo, $"goods_id" === $"virtual_goods_id", "left")
          .withColumn("cur_day", F.to_date($"derived_ts"))
          .withColumn("is_brand", brand)
          .withColumn("first_class", firstClass)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "is_brand", "first_class", "region_code", "platform")
          .agg(
            F.approx_count_distinct("domain_userid").alias("product_detail")
          )
          .cache()

        //加购成功
        val cartSuccess1 = cartSuccess
          .join(goodsInfo, $"element_id" === $"virtual_goods_id", "left")
          .withColumn("cur_day", F.to_date($"derived_ts"))
          .withColumn("is_brand", brand)
          .withColumn("first_class", firstClass)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "is_brand", "first_class", "region_code", "platform")
          .agg(
            F.approx_count_distinct("domain_userid").alias("cart_success")
          )
          .cache()
        //加购
        val cart1 = cart
          .join(goodsInfo, $"element_id" === $"virtual_goods_id", "left")
          .withColumn("cur_day", F.to_date($"derived_ts"))
          .withColumn("is_brand", brand)
          .withColumn("first_class", firstClass)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "is_brand", "first_class", "region_code", "platform")
          .agg(
            F.approx_count_distinct("domain_userid").alias("cart")
          )
          .cache()
        //求gmv
        gmvDf
          .withColumn("is_brand", brand)
          .withColumn("first_class", firstClass)
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "is_brand", "first_class", "region_code", "platform")
          .agg(
            F.approx_count_distinct("pay_user").alias("pay_user"),
            F.approx_count_distinct("pay_num").alias("pay_num"),
            F.sum("gmv").alias("gmv")
          )
          .cache()
        //求dau
        dauDf
          .withColumn("region_code", regionCode)
          .withColumn("platform", platform)
          .groupBy("cur_day", "region_code", "platform")
          .agg(
            F.sum("dau").alias("dau")
          )
          .cache()

        val data = dauDf
          .join(gmvDf, Seq("cur_day", "platform", "region_code"), "left")
          .join(ctr1, Seq("cur_day", "first_class", "is_brand", "platform", "region_code"), "left")
          .join(cart1, Seq("cur_day", "first_class", "is_brand", "platform", "region_code"), "left")
          .join(productDetail1, Seq("cur_day", "first_class", "is_brand", "platform", "region_code"), "left")
          .join(cartSuccess1, Seq("cur_day", "first_class", "is_brand", "platform", "region_code"), "left")
          .select(
            F.coalesce($"cur_day", F.lit("0000-00-00")).alias("cur_day"),
            F.coalesce($"first_class", F.lit("")).alias("first_class"),
            F.coalesce($"is_brand", F.lit("")).alias("is_brand"),
            F.coalesce($"platform", F.lit("")).alias("platform"),
            F.coalesce($"region_code", F.lit("")).alias("region_code"),
            F.coalesce($"dau", F.lit(0)).alias("dau"),
            F.coalesce($"pay_user", F.lit(0)).alias("pay_user"),
            F.coalesce($"pay_num", F.lit(0)).alias("pay_num"),
            F.coalesce($"gmv", F.lit(0.00)).alias("gmv"),
            F.coalesce($"sum_impressions", F.lit(0)).alias("sum_impressions"),
            F.coalesce($"sum_clicks", F.lit(0)).alias("sum_clicks"),
            F.coalesce($"product_detail", F.lit(0)).alias("product_detail"),
            F.coalesce($"cart", F.lit(0)).alias("cart"),
            F.coalesce($"cart_success", F.lit(0)).alias("cart_success")
          )
          .cache()

        //data.show(false)
        reportDb.insertPure("brand_report", data, spark)
      }
      start = next
    }

  }

}
