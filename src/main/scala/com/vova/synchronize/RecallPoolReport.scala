package com.vova.synchronize

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.vova.conf.Conf
import com.vova.db.DataSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object EventType {
  val PLACE_ORDER = 1
  val PAID = 2
}

case class OrderCause(
                       event_type: Int,
                       recall_pool: String,
                       goods_id: Int,
                       user_id: Long,
                       country: String,
                       platform: String,
                       timestamp: Long,
                       order_goods_gmv: Double
                     )

object RecallPoolReport {

  def createTable: String = {
    """
      |CREATE TABLE `recall_pool_report`
      |(
      |    `id`                 bigint AUTO_INCREMENT,
      |    `event_time`         timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
      |    `is_single`          varchar(8)         DEFAULT '',
      |    `region_code`        varchar(4)         DEFAULT '',
      |    `platform`           varchar(8)         DEFAULT '',
      |    `recall_pool`        varchar(64)        DEFAULT '召回集',
      |    `recall_name`        varchar(48)        DEFAULT '召回名称',
      |    `recall_times`       int                DEFAULT 0 COMMENT '召回次数',
      |    `clicks`             int                DEFAULT 0 COMMENT '点击',
      |    `impressions`        int                DEFAULT 0 COMMENT '曝光',
      |    `add_to_bag_success` int                DEFAULT 0 COMMENT '加车成功数',
      |    `gmv`                decimal(10, 2)     DEFAULT 0 COMMENT 'gmv',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`event_time`, `platform`, `region_code`, `recall_pool`, `is_single`, `recall_name`),
      |    KEY `recall_pool` (`recall_pool`),
      |    KEY `region_code` (`region_code`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='召回集报表';
    """.stripMargin
  }

  def isSingle(recallPool: String): Boolean = {
    recallPool.count(_ == '1') == 1
  }

  def getRecallName(recallPool: String, index: Int): String = {
    if (recallPool.length >= 32 && recallPool.charAt(index) == '1') {
      index match {
        case 0 => "click"
        case 1 => "order"
        case 2 => "search_top"
        case 3 => "sell_top"
        case 4 => "non_brand"
        case 5 => "cat_recall_sell"
        case 6 => "cat_recall_low_price"
        case 7 => "cat_recall_discount"
        case 8 => "gender_recall"
        case 9 => "new_user"
        case _ => "no_matched"
      }
    } else {
      "no_matched"
    }
  }

  def loadBatchProcessed(path: String, spark: SparkSession): DataFrame = {
    println(Conf.getString(" s3.evt.batch.root") + path + "T*")
    spark.read
      .json(Conf.getString(" s3.evt.batch.root") + path + "T*")
  }


  def transform(path: String, spark: SparkSession): Unit = {
    import spark.implicits._
    val recallType = spark.udf.register("is_single_func", isSingle(_: String): Boolean)
    val recallName = spark.udf.register("get_recall_name", getRecallName(_: String, _: Int): String)

    val rawHit = loadBatchProcessed("hit/" + path, spark)
      .filter($"recall_pool".isNotNull && F.length($"recall_pool") > 0 && $"goods_id".isNotNull && $"os".isin("android", "ios"))
      .select("recall_pool", "url", "referrer", "user_unique_id", "os", "country", "derived_ts", "page_code", "event_name", "element_name")
      .withColumn("goods_id", F.regexp_extract($"url", "goods_id=([0-9]+)", 1))
      .withColumnRenamed("os", "platform")
      .withColumn("event_time", F.to_utc_timestamp($"derived_ts", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("event_time", F.date_format($"event_time", "yyyy-MM-dd HH:00:00"))
      .withColumnRenamed("user_unique_id", "user_id")
      .withColumn("is_single", recallType($"recall_pool"))

    rawHit.show(false)


    val start = path.replace('/', '-')
    val end = path.replace('/', '-') + " 23:59:59"
    val sql =
      s"""
         |SELECT oi.order_time,
         |       r.region_code                            AS country,
         |       CASE
         |           WHEN ore.device_type = 11 THEN 'ios'
         |           WHEN ore.device_type = 12 THEN 'android'
         |           END                                  AS platform,
         |       oi.user_id,
         |       vg.virtual_goods_id,
         |       (og.shop_price_amount + og.shipping_fee) AS gmv
         |FROM order_goods og
         |         JOIN order_info oi
         |              ON oi.order_id = og.order_id
         |         JOIN order_relation ore USING (order_sn)
         |         JOIN virtual_goods vg
         |              ON vg.goods_id = og.goods_id
         |         JOIN region r ON r.region_id = oi.country
         |         JOIN goods g ON g.goods_id = vg.goods_id
         |WHERE oi.parent_order_id = 0
         |  AND oi.order_time >= '$start'
         |  AND oi.order_time < '$end'
         |  AND oi.pay_status >= 1
         |  AND ore.device_type IN (11, 12)
       """.stripMargin
    val themisDb = new DataSource("themis_read")

    val df1 = themisDb.load(spark, sql)
      .map(row =>
        OrderCause(
          EventType.PAID,
          recall_pool = null,
          goods_id = row.getAs[Int]("virtual_goods_id"),
          user_id = row.getAs[Long]("user_id"),
          country = row.getAs[String]("country"),
          platform = row.getAs[String]("platform"),
          timestamp = row.getAs[Timestamp]("order_time").toLocalDateTime.toEpochSecond(ZoneOffset.UTC),
          order_goods_gmv = row.getDecimal(5).doubleValue())
      )
    val df2 = rawHit
      .filter($"element_name" === "pdAddToCartSuccess" && $"event_name" === "common_click" && $"user_id".isNotNull)
      .filter($"platform".isin("android", "ios"))
      .filter($"referrer".like("%homepage%")) //可能有问题
      .map(row =>
      OrderCause(
        EventType.PLACE_ORDER,
        recall_pool = row.getAs[String]("recall_pool"),
        goods_id = try {
          row.getAs[String]("goods_id").toInt
        } catch {
          case _: Exception => -1
        },
        user_id = row.getAs[String]("user_id").toLong,
        country = row.getAs[String]("country"),
        platform = row.getAs[String]("platform"),
        timestamp = LocalDateTime.parse(row.getAs[String]("derived_ts"), DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC),
        order_goods_gmv = 0.00
      )
    )
    val winSpec = Window
      .partitionBy("user_id", "goods_id")
      .orderBy("timestamp")
    val gmvDf = df1
      .union(df2)
      .withColumn("last_recall_pool", F.last("recall_pool", true).over(winSpec))
      .filter($"event_type" === EventType.PAID && $"last_recall_pool".isNotNull)
      .withColumn("event_time", F.from_unixtime($"timestamp", "yyyy-MM-dd HH:00:00"))
      .withColumn("recall_pool", $"last_recall_pool")
      .withColumn("is_single", recallType($"recall_pool"))


    gmvDf.show(false)
    val rawGoodsCtr = loadBatchProcessed("goods_ctr_v2/" + path, spark)
      .filter($"recall_pool".isNotNull)
      .filter($"os_type".isin("android", "ios"))
      .select("recall_pool", "goods_id", "impressions", "clicks", "user_unique_id", "os_type", "country", "derived_ts")
      .withColumn("is_single", recallType($"recall_pool"))
      .withColumnRenamed("os_type", "platform")
      .withColumn("event_time", F.to_utc_timestamp($"derived_ts", "yyyy-MM-dd HH:00:00"))
      .withColumn("event_time", F.date_format($"event_time", "yyyy-MM-dd HH:00:00"))
      .withColumnRenamed("user_unique_id", "user_id")
      .filter(F.length($"recall_pool") > 0)

    rawGoodsCtr.show(false)
    val reportDb = new DataSource("themis_report_write")
    for {
      platform <- List(F.lit("all"), F.col("platform"))
      country <- List(F.lit("all"), F.col("country"))
      recallPool <- List(F.lit("all"), F.col("recall_pool"))
      single <- List(F.lit("all"), F.col("is_single"))
    } {
      val ctr = rawGoodsCtr
        .withColumn("platform", platform)
        .withColumn("country", country)
        .withColumn("recall_pool", recallPool)
        .withColumn("is_single", single)
        .groupBy("event_time", "platform", "country", "recall_pool", "is_single")
        .agg(
          F.sum($"impressions").alias("impressions"),
          F.sum($"clicks").alias("clicks")
        )

      //recall_times
      val homePage = rawHit
        .filter($"page_code" === "homepage")
        .filter($"event_name" === "screen_view")
        .withColumn("platform", platform)
        .withColumn("country", country)
        .withColumn("recall_pool", recallPool)
        .withColumn("is_single", single)
        .groupBy("event_time", "platform", "country", "recall_pool", "is_single")
        .agg(
          F.count(F.lit(1)).alias("recall_times")
        )

      val pdSuccess = rawHit
        .filter($"element_name" === "pdAddToCartSuccess")
        .filter($"event_name" === "common_click")
        .filter($"referrer".like("%homepage%")) //可能有问题
        .withColumn("platform", platform)
        .withColumn("country", country)
        .withColumn("recall_pool", recallPool)
        .withColumn("is_single", single)
        .groupBy("event_time", "platform", "country", "recall_pool", "is_single")
        .agg(
          F.count(F.lit(1)).alias("add_to_bag_success")
        )

      val gmv = gmvDf
        .withColumn("platform", platform)
        .withColumn("country", country)
        .withColumn("recall_pool", recallPool)
        .withColumn("is_single", single)
        .groupBy("event_time", "platform", "country", "recall_pool", "is_single")
        .agg(
          F.sum("order_goods_gmv").alias("gmv").alias("gmv")
        )
      val data = ctr
        .join(homePage, Seq("event_time", "platform", "country", "recall_pool", "is_single"), "left")
        .join(pdSuccess, Seq("event_time", "platform", "country", "recall_pool", "is_single"), "left")
        .join(gmv, Seq("event_time", "platform", "country", "recall_pool", "is_single"), "left")
        .select(
          $"event_time",
          $"platform",
          $"country",
          $"recall_pool",
          $"is_single",
          F.coalesce($"recall_times", F.lit(0)).alias("recall_times"),
          F.coalesce($"clicks", F.lit(0)).alias("clicks"),
          F.coalesce($"impressions", F.lit(0)).alias("impressions"),
          F.coalesce($"add_to_bag_success", F.lit(0)).alias("add_to_bag_success"),
          F.coalesce($"gmv", F.lit(0.00)).alias("gmv")
        )
        .cache()
      for (i <- 0 to 8) {
        val data1 = data
          .withColumn("recall_name", recallName($"recall_pool", F.lit(i)))
          .filter($"recall_name" =!= "no_matched" && $"recall_pool" =!= "all")
          .withColumnRenamed("country", "region_code")
        if (!data1.isEmpty) {
          data1.show(false)
          reportDb.insertPure("recall_pool_report", data1, spark)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val reportDb = new DataSource("themis_report_write")
    reportDb.execute(createTable)
    val appName = "recall_pool"
    println(appName)
    //    val spark = SparkSession.builder
    //      .appName("recall_pool")
    //      .master("local[*]")
    //      .config("spark.sql.session.timeZone", "UTC")
    //      .getOrCreate()
    //    spark.sparkContext.setLogLevel("WARN")
    //    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    //    val (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))
    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")

    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))
    transform(start.format(dateFormat), spark)
  }
}
