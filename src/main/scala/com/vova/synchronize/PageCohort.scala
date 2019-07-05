package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}

object PageCohort {
  def createTable: String =
    """
      |CREATE TABLE `page_cohort_report`
      |(
      |    `id`          bigint AUTO_INCREMENT,
      |    `cur_day`     date      NOT NULL DEFAULT '0000-00-00',
      |    `region_code` varchar(4)         DEFAULT '',
      |    `platform`    varchar(8)         DEFAULT '',
      |    `page_code`   varchar(32)        DEFAULT '',
      |    `dau`         int                DEFAULT 0 COMMENT '当日进入页面的人数',
      |    `next_1`      int                DEFAULT 0 COMMENT 'dau中次日进入',
      |    `next_3`      int                DEFAULT 0 COMMENT 'dau中第3日进入',
      |    `next_7`      int                DEFAULT 0 COMMENT 'dau中第7日进入',
      |    `next_28`     int                DEFAULT 0 COMMENT 'dau中第28日进入',
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`cur_day`, `platform`, `region_code`, `page_code`),
      |    KEY `region_code` (`region_code`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT = '页面留存报表'
    """.stripMargin

  //  def dropTable: String =
  //    """
  //      |drop table page_cohort_report
  //    """.stripMargin


  def loadBatchData(path: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read
      .json("s3://vomkt-evt/batch-processed/" + path + "T*")
      .withColumn("os_family", F.lower($"os_family"))
      .filter($"os_family".isin("android", "ios"))
      .withColumn("device_id", F.coalesce($"device_id", $"organic_idfv", $"android_id", $"idfa", $"idfv"))
      .withColumn("cur_day", F.to_date($"derived_ts"))
      .withColumnRenamed("country", "region_code")
      .select("os_family", "region_code", "page_code", "event_name", "device_id", "cur_day")
      .withColumnRenamed("os_family", "platform")
  }

  def preFilter(data: DataFrame, pageCode: (String, String), regionCode: Column, platform: Column): DataFrame = {
    data
      .where(pageCode._2)
      .withColumn("page_code", F.lit(pageCode._1))
      .withColumn("region_code", regionCode)
      .withColumn("platform", platform)
  }

  def main(args: Array[String]): Unit = {

    val reportDb = new DataSource("themis_report_write")
    //reportDb.execute(createTable)

    //val themisDb = new DataSource("themis_read")
    //    reportDb.execute(dropTable)
    //    reportDb.execute(createTable)


    val appName = "page_cohort"

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

    while (start.compareTo(end) <= 0) {
      val next = start.plusDays(1)
      //原数据
      val dau = loadBatchData("hit/" + start.format(dateFormat), spark).cache()
      val next_1 = {
        if (start.plusDays(1).compareTo(LocalDate.now()) < 0) loadBatchData("hit/" + start.plusDays(1).format(dateFormat), spark)
        else dau.limit(1)
      }
      val next_3 = {
        if (start.plusDays(3).compareTo(LocalDate.now()) < 0) loadBatchData("hit/" + start.plusDays(3).format(dateFormat), spark)
        else dau.limit(1)
      }
      val next_7 = {
        if (start.plusDays(7).compareTo(LocalDate.now()) < 0) loadBatchData("hit/" + start.plusDays(7).format(dateFormat), spark)
        else dau.limit(1)
      }
      val next_28 = {
        if (start.plusDays(28).compareTo(LocalDate.now()) < 0) loadBatchData("hit/" + start.plusDays(28).format(dateFormat), spark)
        else dau.limit(1)
      }

      //需要计算的page_code
      val pageCodes = List(
        ("lucky_check_in", "page_code = 'lucky_checkin' and event_name = 'page_view'"),
        ("lucky_star", "page_code = 'lucky_star' and event_name = 'page_view'")
      )

      for {
        platform <- List(F.col("platform"), F.lit("all"))
        regionCode <- List(F.col("region_code"), F.lit("all"))
        pageCode <- pageCodes
      } {
        val dauDf = preFilter(dau, pageCode, regionCode, platform)
        val cohort_0 = dauDf
          .groupBy("cur_day", "region_code", "platform", "page_code")
          .agg(
            F.approx_count_distinct("device_id").alias("dau")
          )

        val cohort_1 = dauDf
          .join(preFilter(next_1, pageCode, regionCode, platform).select("device_id"), "device_id")
          .groupBy("cur_day", "region_code", "platform", "page_code")
          .agg(
            F.approx_count_distinct("device_id").alias("next_1")
          )

        val cohort_3 = dauDf
          .join(preFilter(next_3, pageCode, regionCode, platform).select("device_id"), "device_id")
          .groupBy("cur_day", "region_code", "platform", "page_code")
          .agg(
            F.approx_count_distinct("device_id").alias("next_3")
          )
        val cohort_7 = dauDf
          .join(preFilter(next_7, pageCode, regionCode, platform).select("device_id"), "device_id")
          .groupBy("cur_day", "region_code", "platform", "page_code")
          .agg(
            F.approx_count_distinct("device_id").alias("next_7")
          )
        val cohort_28 = dauDf
          .join(preFilter(next_28, pageCode, regionCode, platform).select("device_id"), "device_id")
          .groupBy("cur_day", "region_code", "platform", "page_code")
          .agg(
            F.approx_count_distinct("device_id").alias("next_28")
          )

        val dataFinal = cohort_0
          .join(cohort_1, Seq("cur_day", "region_code", "platform", "page_code"), "left")
          .join(cohort_3, Seq("cur_day", "region_code", "platform", "page_code"), "left")
          .join(cohort_7, Seq("cur_day", "region_code", "platform", "page_code"), "left")
          .join(cohort_28, Seq("cur_day", "region_code", "platform", "page_code"), "left")
          .select(
            F.coalesce($"cur_day", F.lit("0000-00-00")).alias("cur_day"),
            F.coalesce($"region_code", F.lit("")).alias("region_code"),
            F.coalesce($"platform", F.lit("")).alias("platform"),
            F.coalesce($"page_code", F.lit("")).alias("page_code"),

            F.coalesce($"dau", F.lit(0)).alias("dau"),
            F.coalesce($"next_1", F.lit(0)).alias("next_1"),
            F.coalesce($"next_3", F.lit(0)).alias("next_3"),
            F.coalesce($"next_7", F.lit(0)).alias("next_7"),
            F.coalesce($"next_28", F.lit(0)).alias("next_28")
          )
          .cache()

        dataFinal.show()

        reportDb.insertPure("ab_report", dataFinal, spark)
      }
      start = next
    }

  }
}
