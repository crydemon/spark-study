package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import org.apache.spark.sql.{SparkSession, functions => F}

object CheckInReport {

  def createTable: String = {
    """
      |CREATE TABLE `lucky_star_check_in_report`
      |(
      |    `id`          bigint AUTO_INCREMENT,
      |    `cur_day`     date      NOT NULL DEFAULT '0000-00-00',
      |    `platform`    varchar(8)  DEFAULT '',
      |    `region_code` varchar(8)  DEFAULT '',
      |    `page_code`   varchar(32)  DEFAULT '',
      |    `check_1`     int         DEFAULT 0,
      |    `check_2`     int         DEFAULT 0,
      |    `check_3`     int         DEFAULT 0,
      |    `check_4`     int         DEFAULT 0,
      |    `check_5`     int         DEFAULT 0,
      |    `check_6`     int         DEFAULT 0,
      |    `check_7`     int         DEFAULT 0,
      |    PRIMARY KEY `id` (`id`),
      |    UNIQUE (`cur_day`, `platform`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='一元夺宝签到记录'
    """.stripMargin
  }

  def dropTable: String =
    """
      |drop table lucky_star_check_in_report
    """.stripMargin


  def main(args: Array[String]): Unit = {

    val reportDb = new DataSource("themis_report_write")
//    reportDb.execute(dropTable)
//    reportDb.execute(createTable)

    val themisDb = new DataSource("themis_read")
    val appName = "lucky_star_check_in"

    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")

    //        val spark = SparkSession.builder
    //          .appName(appName)
    //          .master("local[*]")
    //          .getOrCreate()
    //        spark.sparkContext.setLogLevel("WARN")


    import spark.implicits._
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))


    while (start.compareTo(end) <= 0) {
      val next = start.plusDays(1)

      val startS = start.format(dateFormat).replace("/", "-")
      val endS = next.format(dateFormat).replace("/", "-")

      val checkInSql =
        s"""
           |SELECT date(lcirr.last_check_in_time)   AS cur_day,
           |       platform,
           |       concat('check_', check_in_times) AS check_in_times,
           |       count(DISTINCT lcirr.user_id)    AS check_nums
           |FROM luckystar_check_in_round_record lcirr
           |         INNER JOIN app_install_record air USING (user_id)
           |WHERE lcirr.last_check_in_time >= '$startS'
           |  AND lcirr.last_check_in_time < '$endS'
           |GROUP BY concat('check_', check_in_times), cur_day, platform
       """.stripMargin

      println(checkInSql)

      val rawDataCheck = themisDb.load(spark, checkInSql)
      for {
        platform <- List(F.col("platform"), F.lit("all"))
      } {
        val dataFinal = rawDataCheck
          .withColumn("platform", platform)
          .groupBy("cur_day", "platform")
          .pivot("check_in_times")
          .sum("check_nums")
          .na.fill(0)
          .withColumn("region_code", F.lit("all"))
          .withColumn("page_code", F.lit("lucky_checkin"))
          .orderBy("cur_day")
          .cache()
        dataFinal.show()
        reportDb.insertPure("lucky_star_check_in_report", dataFinal, spark)
      }
      start = next
    }

  }
}
