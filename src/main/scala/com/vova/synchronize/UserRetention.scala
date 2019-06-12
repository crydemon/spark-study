package com.vova.synchronize

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.vova.snowplow.schema.VovaEvent
import com.vova.utils.S3
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}

case class ActivityUser(activityName: String, deviceId: String)
//活动的意义什么
//活动留存的意义是什么
object UserRetention {


  def loadData(spark: SparkSession, start: LocalDate, end: LocalDate): Unit = {
    import spark.implicits._
    def transfer(data: Dataset[Event]): Dataset[ActivityUser] = {
      data.flatMap { row =>
        val e = new VovaEvent(row)
        (e.event_name, e.common_context.page_code) match {
          case ("page_view", "luckystar") => Some(ActivityUser("luckystar", e.domain_userid))
          case _ => None
        }
      }
    }

    var curDay = start
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val savePath = "d://vomkt-emr-rec/testresult/d_1199/"
    val s3DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
    while (curDay.compareTo(end) <= 0) {
      //当天用户
      println(curDay.toString)
      var day = curDay
      var next = day.plusDays(1)
      val day0User = transfer(S3.loadEnrichData(spark, LocalDateTime.parse(day.format(dateFormat) + "-00", s3DateFormat), LocalDateTime.parse(next.format(dateFormat) + "-00", s3DateFormat)))
          .cache()
      //1天前
      day = curDay.plusDays(1)
      next = day.plusDays(1)
      val day1User = transfer(S3.loadEnrichData(spark, LocalDateTime.parse(day.format(dateFormat) + "-00", s3DateFormat), LocalDateTime.parse(next.format(dateFormat) + "-00", s3DateFormat)))
        .cache()
      //7天前
      day = curDay.plusDays(7)
      next = day.plusDays(1)
      val day7User = transfer(S3.loadEnrichData(spark, LocalDateTime.parse(day.format(dateFormat) + "-00", s3DateFormat), LocalDateTime.parse(next.format(dateFormat) + "-00", s3DateFormat)))
          .cache()

      day7User.createOrReplaceTempView("day_7_user")
      day1User.createOrReplaceTempView("day_1_user")
      day0User.createOrReplaceTempView("day_0_user")
      spark.sql(
        """
          |select
          | *
          |from day_0_user d0
          | left join da
        """.stripMargin)


      curDay = curDay.plusDays(1)
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
}
