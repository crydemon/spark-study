package com.vova.causeanalytic

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.vova.conf.Conf
import com.vova.db.DataSource
import com.vova.snowplow.schema.VovaEventHelper
import com.vova.utils.S3
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}

import scala.util.Try

object CauseAnaliticEventType {
  val goods_click = 0
  val place_order = 1
  val add_to_cart = 2
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

object Main {

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

  def filterUtf8m4(s: String): String = {
    if (s == null) s else s.replaceAll("[^\\u0000-\\uD7FF\\uE000-\\uFFFF]", "?")
  }

  def loadS3Events(spark: SparkSession, start: LocalDateTime, end: LocalDateTime): Dataset[CauseAnaliticEvent] = {
    import spark.implicits._
    S3.loadEnrichData(spark, start.minus(1, ChronoUnit.HOURS), end)
      .transform(S3.dedup)
      .flatMap { row =>
        val e = new VovaEventHelper(row)
        val uid = e.common_context.user_unique_id
        if (uid <= 0) {
          None
        } else {
          e.event_name match {
            case "goods_click" => {
              val goods_id = e.goods_click_event.goods_id
              if (goods_id <= 0) {
                None
              } else {
                Some(CauseAnaliticEvent(
                  event_type = CauseAnaliticEventType.goods_click,
                  timestamp = LocalDateTime.parse(e.derived_ts, DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC),
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
                  page_code = e.common_context.page_code,
                  list_type = filterUtf8m4(e.goods_click_event.list_type),
                  list_uri = filterUtf8m4(e.goods_click_event.list_uri),
                  order_id = -1,
                  order_goods_rec_id = -1,
                  order_goods_gmv = 0
                ))
              }
            }
            case "common_click" => {
              val goods_id = Try(e.common_click_event.element_id.toInt).getOrElse(-1)
              if (goods_id <= 0 || e.common_click_event.element_name != "pdAddToCartSuccess") {
                None
              } else {
                Some(CauseAnaliticEvent(
                  event_type = CauseAnaliticEventType.add_to_cart,
                  timestamp = LocalDateTime.parse(e.derived_ts, DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC),
                  uid_short = uid,
                  domain_userid = e.domain_userid,
                  device_id = e.app_common_context.device_id,
                  gender = e.common_context.gender,
                  country = e.common_context.country,
                  platform = e.platform,
                  os_type = e.os_type,
                  app_version = e.app_common_context.app_version,
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
  }

  def run(spark: SparkSession, start: LocalDateTime, end: LocalDateTime): Unit = {
    import spark.implicits._
    val timeUdf = F.udf((secs: Long) => LocalDateTime.ofEpochSecond(secs, 0, ZoneOffset.UTC).toString)
    val winspec = Window
      .partitionBy("uid_short", "virtual_goods_id")
      .orderBy("timestamp")
    val df = loadS3Events(spark, start, end)
      .union(loadDbEvents(spark, start, end))
      .withColumn("last_click_list_uri",
        F.last("list_uri", ignoreNulls = true).over(winspec)
      )
      .withColumn("last_click_list_type",
        F.last("list_type", ignoreNulls = true).over(winspec)
      )
      .withColumn("last_click_page_code",
        F.last("page_code", ignoreNulls = true).over(winspec)
      ).cache()
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

    val inFmt = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    val (start, end) = (LocalDateTime.parse(args(0), inFmt), LocalDateTime.parse(args(1), inFmt))
    run(spark, start, end)

    spark.stop()
  }
}
