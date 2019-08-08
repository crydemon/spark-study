package com.vova.synchronize

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

import com.vova.conf.Conf
import com.vova.db.DataSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}

import scala.util.Try

object CauseAnaliticEventType {
  val goods_click = 0
  val place_order = 1
  val add_to_cart = 2
  val ordered = 3
  val payed = 4
  val refund = 5
}

case class CauseAnaliticEvent(
                               event_type: Int,
                               timestamp: Long,
                               uid_short: Long,
                               domain_userid: String,
                               device_id: String,
                               gender: String,
                               country: String,
                               platform: String,
                               os_type: String,
                               app_version: String,
                               virtual_goods_id: Long,
                               goods_sn: String,
                               page_code: String,
                               list_type: String,
                               list_uri: String,
                               order_id: Long,
                               order_goods_rec_id: Long,
                               order_goods_gmv: Double
                             )

object OrderAnalytic {

  private val mysql_time_fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00")

  def loadDbEvents(spark: SparkSession, start: LocalDateTime, end: LocalDateTime): Dataset[CauseAnaliticEvent] = {
    import spark.implicits._
    val themisDb = new DataSource("themis_read")
    println("load order goods:", start.format(mysql_time_fmt), end.format(mysql_time_fmt))
    themisDb.load(spark,
      s"""
         |select
         |  oi.order_time,
         |  oi.user_id,
         |  vg.virtual_goods_id,
         |  g.goods_sn,
         |  oi.order_id,
         |  og.rec_id,
         |  (og.shop_price_amount + og.shipping_fee) as gmv
         |from order_goods og
         |join order_info oi
         |  on oi.order_id = og.order_id
         |join virtual_goods vg
         |  on vg.goods_id = og.goods_id
         |join goods g on g.goods_id = vg.goods_id
         |  and oi.order_time >= '${start.format(mysql_time_fmt)}' and oi.order_time < '${end.format(mysql_time_fmt)}'
         |where oi.parent_order_id = 0
         |  and oi.email not regexp '@i9i8.com|@tetx.com|@vova.com.hk|@airydress.com'
            """.stripMargin)
      .map { row =>
        CauseAnaliticEvent(
          event_type = CauseAnaliticEventType.place_order,
          timestamp = {
            val t = row.getAs[java.sql.Timestamp](0)
            if (t == null) 0L else t.toLocalDateTime.toEpochSecond(ZoneOffset.UTC)
          },
          uid_short = row.getAs[Number](1).longValue,
          domain_userid = null,
          device_id = null,
          gender = null,
          country = null,
          platform = null,
          os_type = null,
          app_version = null,
          virtual_goods_id = row.getAs[Number](2).longValue,
          goods_sn = row.getAs[String](3),
          page_code = null,
          list_type = null,
          list_uri = null,
          order_id = row.getAs[Number](4).longValue,
          order_goods_rec_id = row.getAs[Number](5).longValue,
          order_goods_gmv = row.getAs[Number](6).doubleValue
        )
      }
  }

  def loadBatchData(path: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read
      .json("s3://vomkt-evt/batch-processed/" + path + "T*")
      //.json("D:\\vomkt-evt\\batch-processed\\hit\\2019\\06\\23")
  }

  def filterUtf8m4(s: String): String = {
    if (s == null) s else s.replaceAll("[^\\u0000-\\uD7FF\\uE000-\\uFFFF]", "?")
  }

  def loadS3Events(spark: SparkSession, start: LocalDateTime): Dataset[CauseAnaliticEvent] = {
    import spark.implicits._
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val data = loadBatchData("hit/" + start.toLocalDate.format(dateFormat), spark)
      .filter(F.regexp_extract($"user_unique_id", "[0-9]+", 0).isNotNull)
      .filter($"event_name".isin("goods_click", "common_click"))
    data.flatMap { row =>
      val uid = row.getAs[String]("user_unique_id").toInt
      val event_name = row.getAs[String]("event_name")
      val derived_ts = row.getAs[String]("derived_ts")
      val page_code = row.getAs[String]("page_code")
      val list_type = row.getAs[String]("list_type")
      val list_uri = row.getAs[String]("list_uri")
      event_name match {
        case "goods_click" => {
          val goods_id =  Try(row.getAs[String]("goods_id").toInt).getOrElse(-1)
          if (goods_id <= 0) {
            None
          } else {
            Some(CauseAnaliticEvent(
              event_type = CauseAnaliticEventType.goods_click,
              timestamp = LocalDateTime.parse(derived_ts, DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC),
              uid_short = uid,
              domain_userid = null,
              device_id = null,
              gender = null,
              country = null,
              platform = null,
              os_type = null,
              app_version = null,
              virtual_goods_id = goods_id,
              goods_sn = null,
              page_code = page_code,
              list_type = filterUtf8m4(list_type),
              list_uri = filterUtf8m4(list_uri),
              order_id = -1,
              order_goods_rec_id = -1,
              order_goods_gmv = 0
            ))
          }
        }
        case "common_click" => {
          val goods_id = Try(row.getAs[String]("element_id").toInt).getOrElse(-1)
          val element_name = row.getAs[String]("element_name")
          if (goods_id <= 0 || element_name != "pdAddToCartSuccess") {
            None
          } else {
            Some(CauseAnaliticEvent(
              event_type = CauseAnaliticEventType.add_to_cart,
              timestamp = LocalDateTime.parse(derived_ts, DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC),
              uid_short = uid,
              domain_userid = row.getAs[String]("domain_userid"),
              device_id = row.getAs[String]("device_id"),
              gender = row.getAs[String]("gender"),
              country = row.getAs[String]("country"),
              platform = row.getAs[String]("platform"),
              os_type = row.getAs[String]("os"),
              app_version = row.getAs[String]("app_version"),
              virtual_goods_id = goods_id,
              goods_sn = null,
              page_code = null, // we don't care page_code of addToCart, set to null in order to compute last_click_page_code
              list_type = null,
              list_uri = null,
              order_id = -1,
              order_goods_rec_id = -1,
              order_goods_gmv = 0
            ))
          }
        }
        case _ => None
      }
    }
  }

  def run(spark: SparkSession, start: LocalDateTime, end: LocalDateTime): Unit = {
    import spark.implicits._
    val timeUdf = F.udf((secs: Long) => LocalDateTime.ofEpochSecond(secs, 0, ZoneOffset.UTC).toString)
    val winspec = Window
      .partitionBy("uid_short", "virtual_goods_id")
      .orderBy("timestamp")
    val df = loadS3Events(spark, start)
      .union(loadS3Events(spark, start.minusDays(1)))
      .union(loadDbEvents(spark, start, end))
      .withColumn("last_click_list_uri",
        F.last("list_uri", ignoreNulls = true).over(winspec)
      )
      .withColumn("last_click_list_type",
        F.last("list_type", ignoreNulls = true).over(winspec)
      )
      .withColumn("last_click_page_code",
        F.last("page_code", ignoreNulls = true).over(winspec)
      )
      .cache()
    df.show(false)
    val df_cart = df
      .filter($"event_type" === CauseAnaliticEventType.add_to_cart)
      .select(
        timeUdf($"timestamp").as("add_cart__time"),
        F.substring(F.coalesce($"last_click_page_code", F.lit("")), 0, 32).as("last_click_page_code"),
        F.substring(F.coalesce($"last_click_list_type", F.lit("")), 0, 32).as("last_click_list_type"),
        F.substring(F.coalesce($"last_click_list_uri", F.lit("")), 0, 64).as("last_click_list_uri"),
        $"os_type",
        $"virtual_goods_id",
        $"uid_short".as("user_id"),
        $"domain_userid",
        $"platform",
        $"app_version",
        $"country",
        $"device_id",
        $"gender"
      )
    //        df_cart.show(false)
    val reportDb = new DataSource("themis_report_write")
    reportDb.insertPure("temp_cart_cause", df_cart, spark)
    val df_order = df
      .filter($"event_type" === CauseAnaliticEventType.place_order)
      .select(
        $"order_goods_rec_id",
        timeUdf($"timestamp").as("order_time"),
        $"uid_short".as("user_id"),
        $"virtual_goods_id",
        $"order_goods_gmv",
        F.substring(F.coalesce($"last_click_list_uri", F.lit("")), 0, 64).as("last_click_list_uri"),
        F.substring(F.coalesce($"last_click_list_type", F.lit("")), 0, 32).as("last_click_list_type"),
        F.substring(F.coalesce($"last_click_page_code", F.lit("")), 0, 32).as("last_click_page_code"),
        $"order_id",
        $"goods_sn"
      )
    reportDb.insertPure("temp_order_cause", df_order, spark)
  }

  def main(args: Array[String]): Unit = {
    val appName = "cause-analytic"
    println(appName)
    val spark = SparkSession.builder
      .master(Conf.getString("spark.master"))
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sparkContext.setCheckpointDir(Conf.getString("s3.rec.checkpoint"))

    //    val spark = SparkSession.builder
    //      .appName("hitTest")
    //      .master("local[6]")
    //      .config("spark.executor.cores", 2)
    //      .config("spark.sql.shuffle.partitions", 30)
    //      .config("spark.default.parallelism", 18)
    //      .getOrCreate()
    //    spark.sparkContext.setLogLevel("WARN")

    val inFmt = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    val (start, end) = (LocalDateTime.parse(args(0), inFmt), LocalDateTime.parse(args(1), inFmt))
    run(spark, start, end)
    //    val data = loadS3Events(spark, start)
    //    data.show(false)
    spark.stop()
  }
}
