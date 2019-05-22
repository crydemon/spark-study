package com.vova.export

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.vova.snowplow.schema.cx.Common
import com.vova.snowplow.schema.ue.CommonClick
import com.vova.utils.S3
import io.circe.JsonObject
import org.apache.spark.sql.{SaveMode, SparkSession, functions}


object cmsEvent {
  val VENDOR_ARTEMIS = "com.artemis"
  val VENDOR_SNOWPLOW = "com.snowplowanalytics.snowplow"
  val VENDOR_GOOGLE = "com.google.analytics"
}

class cmsEvent(val event: Event) {
  def getContext(vendor: String, name: String): JsonObject = {
    event.contexts.data.find(data => {
      data.schema.name == name && data.schema.vendor == vendor
    }).flatMap(_.data.asObject).orNull
  }

  def getUnstructEvent(vendor: String, name: String): JsonObject = {
    val data = event.unstruct_event.data.orNull
    if (data != null && data.schema.name == name && data.schema.vendor == vendor) {
      data.data.asObject.orNull
    } else {
      null
    }
  }

  lazy val common_context: Common = new Common(getContext(cmsEvent.VENDOR_ARTEMIS, "common"))
  lazy val platform: String = event.platform.orNull
  lazy val domain_userid: String = Option(common_context.domain_user_id)
    .map(_.trim).filter(_.nonEmpty)
    .orElse(event.domain_userid)
    .orNull

  lazy val raw_event_name: String = event.event_name.getOrElse("")

  lazy val event_name: String = raw_event_name.split("-").head

  lazy val common_click_event: CommonClick = new CommonClick(getUnstructEvent(cmsEvent.VENDOR_ARTEMIS, "common_click-link_click"))
}

case class cmsEvents(domain_userid: String, country: String, tag: String)

object Demand_1199 {


  def loadData(spark: SparkSession, start: LocalDate, end: LocalDate): Unit = {
    import spark.implicits._
    var curDay = start
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val savePath = "s3://vomkt-emr-rec/testresult/d_1199/"
    val s3DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
    while (curDay.compareTo(end) <= 0) {
      println(curDay.toString)
      val nextDay = curDay.plusDays(1)
      val s3Start = LocalDateTime.parse(curDay.format(dateFormat) + "-00", s3DateFormat)
      val s3End = LocalDateTime.parse(nextDay.format(dateFormat) + "-00", s3DateFormat)
      val enrichData = S3.loadEnrichData(spark, s3Start, s3End)
        .flatMap { row =>
          val e = new cmsEvent(row)
          (e.platform, e.event_name, e.common_context.page_code, e.common_context.country, e.common_context.uri, e.common_click_event.element_name) match {
            case ("mob", _, "homepage", country, _, _) => Some(cmsEvents(e.domain_userid, country, "dau"))
            case ("mob", "screen_view", "product_detail", country, _, _) => Some(cmsEvents(e.domain_userid, country, "proudct_detail"))
            case ("mob", "screen_view", "cart", country, _, _) => Some(cmsEvents(e.domain_userid, country, "cart"))
            case ("mob", "order_process", _, country, _, "button_cart_checkout") => Some(cmsEvents(e.domain_userid, country, "cart_buy_confirm"))
            case ("mob", "screen_view", "button_cart_checkout", country, app_uri, _) => if (app_uri != null && app_uri.contains("login_type=login")) Some(cmsEvents(e.domain_userid, country, "login_tab")) else None
            case ("mob", "screen_view", "checkout", country, _, _) => Some(cmsEvents(e.domain_userid, country, "checkout"))
            case ("mob", "screen_view", "login_and_register", country, app_uri, _) => if (app_uri != null && app_uri.contains("login_type=register")) Some(cmsEvents(e.domain_userid, country, "register_tab")) else None
            case ("mob", "order_process", _, country, _, "button_checkout_placeOrder") => Some(cmsEvents(e.domain_userid, country, "checkout_place_order"))
            case ("mob", "screen_view", "payment_cod_verify", country, _, _) => Some(cmsEvents(e.domain_userid, country, "payment_cod_verify"))
            case ("mob", "screen_view", "payment_success", country, _, _) => Some(cmsEvents(e.domain_userid, country, "payment_success"))
            case _ => None
          }
        }
      enrichData.write.mode(SaveMode.Overwrite).parquet(savePath + "tmp/")

      val dataLayer1 = spark.read.parquet(savePath + "tmp/")
      dataLayer1.cache()
      val countryS = Seq("all", "FR", "DE", "IT", "GB", "US", "ID", "BE")
      enrichData.show(truncate = false)
      countryS.foreach(country => {
        val dataLayer2 = country match {
          case "all" => dataLayer1
          case code => dataLayer1.filter($"country" === code)
        }

        val dataLayer3 = dataLayer2
          .groupBy("tag")
          .agg(functions.approx_count_distinct("domain_userid").alias("uv"))
          .withColumn("event_date", functions.lit(curDay.format(dateFormat)))
          .withColumn("country_code", functions.lit(country))
        dataLayer3.cache()
        dataLayer3.show(truncate = false)
        dataLayer3.coalesce(1).write.mode(SaveMode.Append).parquet(savePath + "data/")
      })
      curDay = nextDay
    }
    spark.read
      .parquet(savePath + "data/")
      .groupBy("event_date", "country_code")
      .pivot("tag", Seq("dau", "proudct_detail", "cart", "cart_buy_confirm", "login_tab", "register_tab", "checkout", "checkout_place_order", "payment_cod_verify", "payment_success"))
      .sum("uv")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Append)
      .csv(savePath + "data_final/")
  }

  def main(args: Array[String]): Unit = {
    val appName = "d_1199"
    println(appName)
        val spark = SparkSession.builder
          .master("yarn")
          .appName(appName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.yarn.maxAppAttempts", 1)
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")

    //local
//    val spark = SparkSession.builder
//      .master("local[*]")
//      .appName(appName)
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.sql.session.timeZone", "UTC")
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("WARN")

    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start: LocalDate = LocalDate.parse("2019-03-01", dateFormat)
    val end: LocalDate = LocalDate.parse("2019-05-21", dateFormat)
    loadData(spark, start, end)
    spark.stop()
  }
}


