package com.vova.export


import java.text.SimpleDateFormat
import java.util.Calendar

import com.vova.db.DataSource
import org.apache.spark.sql.SparkSession

object Marketing {


  def loadDataUTCAmount(spark: SparkSession, start: String, end: String) = {
    val pattern = "yyyy-MM-dd"
    val fmt = new SimpleDateFormat(pattern)
    val calendar = Calendar.getInstance()
    calendar.setTime(fmt.parse(start))
    val endDate = fmt.parse(end)
    val reportDb = new DataSource("themis_report_read")
    while (calendar.getTime.compareTo(endDate) <= 0) {
      val activate_date = fmt.format(calendar.getTime)
      println(activate_date)
      calendar.add(Calendar.DATE, 10)
      val end_date = fmt.format(calendar.getTime)
      val sql =
        s"""
           |select date(su.event_time) AS activate_date,
           |       ar.campaign,
           |       ar.media_source,
           |       count(1) as activate_num
           |from app_event_log_user_start_up su
           | left join appsflyer_record ar using(device_id)
           |where su.event_time >= '$activate_date'
           |  and su.event_time < '$end_date'
           |group by activate_date, ar.campaign
                """.stripMargin

      val activateInfo = reportDb.load(spark, sql)
      val orderSql =
        s"""
           |select date(activate_time) AS activate_date,
           |       ar.campaign,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 1, tcocd.order_amount, 0)) as 1_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 2, tcocd.order_amount, 0)) as 2_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 3, tcocd.order_amount, 0)) as 3_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 4, tcocd.order_amount, 0)) as 4_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 5, tcocd.order_amount, 0)) as 5_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 6, tcocd.order_amount, 0)) as 6_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 7, tcocd.order_amount, 0)) as 7_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 8, tcocd.order_amount, 0)) as 8_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 9, tcocd.order_amount, 0)) as 9_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 10, tcocd.order_amount, 0)) as 10_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 11, tcocd.order_amount, 0)) as 11_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 12, tcocd.order_amount, 0)) as 12_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 13, tcocd.order_amount, 0)) as 13_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 14, tcocd.order_amount, 0)) as 14_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 21, tcocd.order_amount, 0)) as 21_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 28, tcocd.order_amount, 0)) as 28_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 56, tcocd.order_amount, 0)) as 56_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 84, tcocd.order_amount, 0)) as 84_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 112, tcocd.order_amount, 0)) as 112_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 140, tcocd.order_amount, 0)) as 140_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 168, tcocd.order_amount, 0)) as 168_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 196, tcocd.order_amount, 0)) as 196_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 224, tcocd.order_amount, 0)) as 224_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 252, tcocd.order_amount, 0)) as 252_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 280, tcocd.order_amount, 0)) as 280_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 308, tcocd.order_amount, 0)) as 308_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 336, tcocd.order_amount, 0)) as 336_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 364, tcocd.order_amount, 0)) as 364_day
           |from temp_country_order_cohort_details tcocd
           |   inner join appsflyer_record ar using (device_id)
           |where activate_time >= '$activate_date'
           |  and activate_time < '$end_date'
           |  and activate_time <= pay_time
           |group by activate_date, ar.campaign
                """.stripMargin

      val orderInfo = reportDb.load(spark, orderSql)
      val data = activateInfo
        .join(orderInfo, Seq("activate_date", "campaign"))

      data.cache() // 之前两个sql同时执行
      Utils.writeToCSV("utc_amount.csv", data, spark)
    }
  }


  def loadDataUTCGMV(spark: SparkSession, start: String, end: String) = {
    import spark.implicits._
    val pattern = "yyyy-MM-dd"
    val fmt = new SimpleDateFormat(pattern)
    val calendar = Calendar.getInstance()
    calendar.setTime(fmt.parse(start))
    val endDate = fmt.parse(end)

    val reportDb = new DataSource("themis_report_read")
    while (calendar.getTime.compareTo(endDate) <= 0) {
      val activate_date = fmt.format(calendar.getTime)
      println(activate_date)
      calendar.add(Calendar.DATE, 10)
      val end_date = fmt.format(calendar.getTime)
      val sql =
        s"""
           |select date(su.event_time) AS activate_date,
           |       ar.campaign,
           |       ar.media_source,
           |       count(1) as activate_num
           |from app_event_log_user_start_up su
           | left join appsflyer_record ar using(device_id)
           |where su.event_time >= '$activate_date'
           |  and su.event_time < '$end_date'
           |group by activate_date, ar.campaign
                """.stripMargin

      val activateInfo = reportDb.load(spark, sql)

      val orderSql =
        s"""
           |SELECT date(activate_time)                            AS activate_date,
           |       ar.campaign,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 1, tcocd.gmv, 0))   AS 1_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 2, tcocd.gmv, 0))   AS 2_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 3, tcocd.gmv, 0))   AS 3_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 4, tcocd.gmv, 0))   AS 4_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 5, tcocd.gmv, 0))   AS 5_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 6, tcocd.gmv, 0))   AS 6_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 7, tcocd.gmv, 0))   AS 7_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 8, tcocd.gmv, 0))   AS 8_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 9, tcocd.gmv, 0))   AS 9_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 10, tcocd.gmv, 0))  AS 10_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 11, tcocd.gmv, 0))  AS 11_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 12, tcocd.gmv, 0))  AS 12_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 13, tcocd.gmv, 0))  AS 13_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 14, tcocd.gmv, 0))  AS 14_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 21, tcocd.gmv, 0))  AS 21_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 28, tcocd.gmv, 0))  AS 28_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 56, tcocd.gmv, 0))  AS 56_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 84, tcocd.gmv, 0))  AS 84_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 112, tcocd.gmv, 0)) AS 112_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 140, tcocd.gmv, 0)) AS 140_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 168, tcocd.gmv, 0)) AS 168_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 196, tcocd.gmv, 0)) AS 196_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 224, tcocd.gmv, 0)) AS 224_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 252, tcocd.gmv, 0)) AS 252_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 280, tcocd.gmv, 0)) AS 280_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 308, tcocd.gmv, 0)) AS 308_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 336, tcocd.gmv, 0)) AS 336_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 364, tcocd.gmv, 0)) AS 364_day
           |FROM temp_country_order_cohort_details tcocd
           |   inner JOIN appsflyer_record ar USING (device_id)
           |WHERE activate_time >= date('$activate_date')
           |  AND activate_time < date('$end_date')
           |GROUP BY tcocd.activate_date, ar.campaign
                """.stripMargin

      val orderInfo = reportDb.load(spark, orderSql)
      val data = activateInfo
        .join(orderInfo, Seq("activate_date", "campaign"))

      data.cache() // 之前两个sql同时执行
      Utils.writeToCSV("utc_gmv.csv", data, spark)
    }
  }

  // cn------------------------------------------------------
  def loadDataCNAmount(spark: SparkSession, start: String, end: String) = {
    val pattern = "yyyy-MM-dd"
    val fmt = new SimpleDateFormat(pattern)
    val calendar = Calendar.getInstance()
    calendar.setTime(fmt.parse(start))
    val endDate = fmt.parse(end)
    val reportDb = new DataSource("themis_report_read")

    while (calendar.getTime.compareTo(endDate) <= 0) {
      val activate_date = fmt.format(calendar.getTime)
      println(activate_date)
      calendar.add(Calendar.DATE, 10)
      val end_date = fmt.format(calendar.getTime)

      val sql =
        s"""
           |select date(date_add(ar.event_time, INTERVAL 8 HOUR )) as activate_date,
           |       ar.campaign,
           |       ar.media_source,
           |       count(1) as activate_num
           |from app_event_log_user_start_up su
           | left join appsflyer_record ar using(device_id)
           |where su.event_time >= date(date_sub('$activate_date', INTERVAL 8 HOUR))
           |  and su.event_time < date(date_sub('$end_date', INTERVAL 8 HOUR))
           |group by activate_date, ar.campaign
                """.stripMargin

      val activateInfo = reportDb.load(spark, sql)
      val orderSql =
        s"""
           |select date(date_add(activate_time, INTERVAL 8 HOUR )) as activate_date,
           |       ar.campaign,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 1, tcocd.order_amount, 0)) as 1_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 2, tcocd.order_amount, 0)) as 2_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 3, tcocd.order_amount, 0)) as 3_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 4, tcocd.order_amount, 0)) as 4_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 5, tcocd.order_amount, 0)) as 5_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 6, tcocd.order_amount, 0)) as 6_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 7, tcocd.order_amount, 0)) as 7_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 8, tcocd.order_amount, 0)) as 8_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 9, tcocd.order_amount, 0)) as 9_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 10, tcocd.order_amount, 0)) as 10_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 11, tcocd.order_amount, 0)) as 11_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 12, tcocd.order_amount, 0)) as 12_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 13, tcocd.order_amount, 0)) as 13_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 14, tcocd.order_amount, 0)) as 14_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 21, tcocd.order_amount, 0)) as 21_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 28, tcocd.order_amount, 0)) as 28_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 56, tcocd.order_amount, 0)) as 56_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 84, tcocd.order_amount, 0)) as 84_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 112, tcocd.order_amount, 0)) as 112_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 140, tcocd.order_amount, 0)) as 140_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 168, tcocd.order_amount, 0)) as 168_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 196, tcocd.order_amount, 0)) as 196_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 224, tcocd.order_amount, 0)) as 224_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 252, tcocd.order_amount, 0)) as 252_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 280, tcocd.order_amount, 0)) as 280_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 308, tcocd.order_amount, 0)) as 308_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 336, tcocd.order_amount, 0)) as 336_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 364, tcocd.order_amount, 0)) as 364_day
           |from temp_country_order_cohort_details tcocd
           |   inner join appsflyer_record ar using (device_id)
           |where  ar.event_time >= date(date_sub('$activate_date', INTERVAL 8 HOUR))
           |  AND ar.event_time < date(date_sub('$end_date', INTERVAL 8 HOUR))
           |  AND activate_time <= pay_time
           |group by activate_date, ar.campaign
                """.stripMargin

      val orderInfo = reportDb.load(spark, orderSql)
      val data = activateInfo
        .join(orderInfo, Seq("activate_date", "campaign"))
      data.cache() // 之前两个sql同时执行
      Utils.writeToCSV("cn_amount.csv", data, spark)
      //reportDbWriter.insertPure("marketing_info_v3", data)
    }
  }

  def loadDataCNGMV(spark: SparkSession, start: String, end: String) = {
    val pattern = "yyyy-MM-dd"
    val fmt = new SimpleDateFormat(pattern)
    val calendar = Calendar.getInstance()
    calendar.setTime(fmt.parse(start))
    val endDate = fmt.parse(end)

    val reportDb = new DataSource("themis_report_read")
    while (calendar.getTime.compareTo(endDate) <= 0) {
      val activate_date = fmt.format(calendar.getTime)
      println(activate_date)
      calendar.add(Calendar.DATE, 10)
      val end_date = fmt.format(calendar.getTime)
      val sql =
        s"""
           |select date(date_add(ar.event_time, INTERVAL 8 HOUR)) AS activate_date,
           |       ar.campaign,
           |       ar.media_source,
           |       count(1) as activate_num
           |from app_event_log_user_start_up su
           | left join appsflyer_record ar using(device_id)
           |where su.event_time >= date(date_sub('$activate_date', INTERVAL 8 HOUR))
           |  and su.event_time < date(date_sub('$end_date', INTERVAL 8 HOUR))
           |group by activate_date, ar.campaign
                """.stripMargin

      val activateInfo = reportDb.load(spark, sql)

      activateInfo
        .groupBy()
      val orderSql =
        s"""
           |SELECT date(date_add(activate_time, INTERVAL 8 HOUR))                            AS activate_date,
           |       ar.campaign,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 1, tcocd.gmv, 0))   AS 1_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 2, tcocd.gmv, 0))   AS 2_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 3, tcocd.gmv, 0))   AS 3_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 4, tcocd.gmv, 0))   AS 4_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 5, tcocd.gmv, 0))   AS 5_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 6, tcocd.gmv, 0))   AS 6_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 7, tcocd.gmv, 0))   AS 7_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 8, tcocd.gmv, 0))   AS 8_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 9, tcocd.gmv, 0))   AS 9_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 10, tcocd.gmv, 0))  AS 10_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 11, tcocd.gmv, 0))  AS 11_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 12, tcocd.gmv, 0))  AS 12_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 13, tcocd.gmv, 0))  AS 13_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 14, tcocd.gmv, 0))  AS 14_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 21, tcocd.gmv, 0))  AS 21_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 28, tcocd.gmv, 0))  AS 28_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 56, tcocd.gmv, 0))  AS 56_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 84, tcocd.gmv, 0))  AS 84_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 112, tcocd.gmv, 0)) AS 112_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 140, tcocd.gmv, 0)) AS 140_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 168, tcocd.gmv, 0)) AS 168_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 196, tcocd.gmv, 0)) AS 196_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 224, tcocd.gmv, 0)) AS 224_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 252, tcocd.gmv, 0)) AS 252_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 280, tcocd.gmv, 0)) AS 280_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 308, tcocd.gmv, 0)) AS 308_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 336, tcocd.gmv, 0)) AS 336_day,
           |       sum(if(hour(timediff(pay_time, activate_time)) < 24 * 364, tcocd.gmv, 0)) AS 364_day
           |FROM temp_country_order_cohort_details tcocd
           |   inner join appsflyer_record ar USING (device_id)
           |WHERE activate_time >= date(date_sub('$activate_date', INTERVAL 8 HOUR))
           |  AND activate_time < date(date_sub('$end_date', INTERVAL 8 HOUR))
           |GROUP BY tcocd.activate_date, ar.campaign
          """.stripMargin

      val orderInfo = reportDb.load(spark, orderSql)
      val data = activateInfo
        .join(orderInfo, Seq("activate_date", "campaign"))

      data.cache() // 之前两个sql同时执行
      Utils.writeToCSV("cn_gmv.csv", data, spark)
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "marketing_info"
    println(appName)
    val spark = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val start = "2018-04-01"
    val end = "2019-05-23"
    //2019-01-16
    //    Marketing.loadDataUTCGMV(spark, start, end)
    //    Marketing.loadDataUTCAmount(spark, start, end)
    Marketing.loadDataCNGMV(spark, start, end)
    Marketing.loadDataCNAmount(spark, start, end)
    spark.stop()
  }
}




