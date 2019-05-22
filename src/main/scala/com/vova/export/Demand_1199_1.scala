package com.vova.export

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.db.DataSource
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object Demand_1199_1 {


  def loadData(spark: SparkSession, start: LocalDate, end: LocalDate): Unit = {
    import spark.implicits._
    var curDay = start

    val savePath = "d:/result"
    FileUtils.deleteDirectory(new File(savePath))

    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    while (curDay.compareTo(end) <= 0) {
      val nextDay = curDay.plusDays(1)
      val startTime = curDay.format(dateFormat)
      val endTime = nextDay.format(dateFormat)

      val themisReader = new DataSource("themis_read")

      val countrys = List[String]("all", "FR", "DE", "IT", "GB", "US", "ID", "BE")
      countrys.foreach(country => {
        val codeWhere = country match {
          case "all" => ""
          case code => s" AND r.region_code = '$code'"
        }
        val payedSql =
          s"""
             |SELECT
             |  date(oi.pay_time) AS cur_day,
             |  '$country' AS country_code,
             |  count(1) AS payed_num,
             |  sum(oi.shipping_fee + oi.goods_amount + oi.duty_fee) AS gmv,
             |  count(DISTINCT oi.user_id) AS payed_user
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.pay_status >= 2
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.pay_time >= '$startTime'
             |  AND oi.pay_time < '$endTime'
             |  $codeWhere
             |GROUP BY cur_day
        """.stripMargin
        val payedInfo = themisReader.load(spark, payedSql)


        payedInfo.cache()


        val orderedSql =
          s"""
             |SELECT
             |  date(oi.order_time) AS cur_day,
             |  count(1) AS ordered_num,
             |  count(DISTINCT oi.user_id) AS ordered_user
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.order_time >= '$startTime'
             |  AND oi.order_time < '$endTime'
             |  $codeWhere
             |GROUP BY cur_day
        """.stripMargin
        val orderedInfo = themisReader.load(spark, orderedSql)
        orderedInfo.cache()

        val pendingSql =
          s"""
             |SELECT
             |  date(oi.pay_time) AS pending_day,
             |  count(1) AS pending_num,
             |  count(DISTINCT oi.user_id) AS pending_user
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.pay_status = 1
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.pay_time >= '$startTime'
             |  AND oi.pay_time < '$endTime'
             |  $codeWhere
             |GROUP BY pending_day
        """.stripMargin
        val pendingInfo = themisReader.load(spark, pendingSql)

        pendingInfo.cache()


        //除去活动
        val excludePayedSql =
          s"""
             |SELECT
             |  date(oi.pay_time) AS cur_day,
             |  count(1) AS payed_num_ex,
             |  sum(oi.shipping_fee + oi.goods_amount + oi.duty_fee) AS gmv_ex,
             |  count(DISTINCT oi.user_id) AS payed_user_ex
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.pay_status >= 2
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.pay_time >= '$startTime'
             |  AND oi.pay_time < '$endTime'
             |  AND NOT exists
             |      (SELECT 1 FROM order_extension oe
             |       WHERE oe.order_id = oi.order_id
             |          AND oe.ext_name IN('daily_gift_activity_id', 'is_free_sale', 'affiliate_activity_id', 'luckystar_activity_id'))
             |  $codeWhere
             |GROUP BY cur_day
        """.stripMargin
        val excludePayedInfo = themisReader.load(spark, excludePayedSql)


        excludePayedInfo.cache()


        val excludeOrderedSql =
          s"""
             |SELECT
             |  date(oi.order_time) AS cur_day,
             |  count(1) AS ordered_num_ex,
             |  count(DISTINCT oi.user_id) AS ordered_user_ex
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.order_time >= '$startTime'
             |  AND oi.order_time < '$endTime'
             |  AND NOT exists
             |    (SELECT 1 FROM order_extension oe
             |     WHERE oe.order_id = oi.order_id
             |        AND oe.ext_name IN('daily_gift_activity_id', 'is_free_sale', 'affiliate_activity_id', 'luckystar_activity_id'))
             |  $codeWhere
             |GROUP BY cur_day
        """.stripMargin
        val excludeOrderedInfo = themisReader.load(spark, excludeOrderedSql)
        excludeOrderedInfo.cache()

        val excludePendingSql =
          s"""
             |SELECT
             |  date(oi.pay_time) AS pending_day_ex,
             |  count(1) AS pending_num_ex,
             |  count(DISTINCT oi.user_id) AS pending_user_ex
             |FROM order_info oi
             |  INNER JOIN region r ON r.region_id = oi.country
             |  INNER JOIN order_relation ore ON ore.order_id = oi.order_id
             |WHERE oi.parent_order_id = 0
             |  AND oi.email NOT REGEXP '@airydress.com|@tetx.com|@qq.com|@i9i8.com'
             |  AND oi.pay_status = 1
             |  AND oi.from_domain LIKE 'api%'
             |  AND oi.pay_time >= '$startTime'
             |  AND oi.pay_time < '$endTime'
             |  AND NOT exists
             |    (SELECT 1 FROM order_extension oe
             |     WHERE oe.order_id = oi.order_id
             |        AND oe.ext_name IN('daily_gift_activity_id', 'is_free_sale', 'affiliate_activity_id', 'luckystar_activity_id'))
             |  $codeWhere
             |GROUP BY pending_day_ex
        """.stripMargin
        val excludePendingInfo = themisReader.load(spark, excludePendingSql)

        excludePendingInfo.cache()

        val allData = orderedInfo.join(payedInfo, "cur_day")
          .join(excludeOrderedInfo, "cur_day")
          .join(excludePayedInfo, "cur_day")
          .join(excludePendingInfo, $"cur_day" === $"pending_day_ex", "left")
          .join(pendingInfo, $"cur_day" === $"pending_day", "left")
          .withColumn("payed_num/ordered_num", functions.round($"payed_num" * 1.0 / $"ordered_num", 2))
          .withColumn("pending_num/ordered_num", functions.round($"pending_num" * 1.0 / $"ordered_num", 2))
          .withColumn("gmv/payed_user", functions.round($"gmv" * 1.0 / $"payed_user", 2))
          .withColumn("gmv/payed_num", functions.round($"gmv" * 1.0 / $"payed_num", 2))

          .withColumn("payed_num_ex/ordered_num_ex", functions.round($"payed_num_ex" * 1.0 / $"ordered_num_ex", 2))
          .withColumn("pending_num_ex/ordered_num_ex", functions.round($"pending_num_ex" * 1.0 / $"ordered_num_ex", 2))
          .withColumn("gmv_ex/payed_user_ex", functions.round($"gmv_ex" * 1.0 / $"payed_user_ex", 2))
          .withColumn("gmv_ex/payed_num_ex", functions.round($"gmv_ex" * 1.0 / $"payed_num_ex", 2))
          .select("cur_day", "country_code", "ordered_num", "ordered_user", "pending_num", "pending_user", "payed_num", "payed_user", "gmv",
            "payed_num/ordered_num", "pending_num/ordered_num", "gmv/payed_user", "gmv/payed_num",
            "ordered_num_ex", "ordered_user_ex", "pending_num_ex", "pending_user_ex", "gmv_ex",
            "payed_num_ex/ordered_num_ex", "pending_num_ex/ordered_num_ex", "gmv_ex/payed_user_ex", "gmv_ex/payed_num_ex"
          )
        allData.cache()

        allData.show(truncate = false)
        //        allData.coalesce(1)
        //          .write
        //          .mode(SaveMode.Append)
        //          .option("header", "true")
        //          .option("delimiter", ",")
        //          .csv(savePath)

        Utils.writeToCSVV2("d_1199", allData, spark)
      })
      curDay = nextDay
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "d_1199_1"
    val spark = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start: LocalDate = LocalDate.parse("2019-05-20", dateFormat)
    val end: LocalDate = LocalDate.parse("2019-05-21", dateFormat)
    //    loadData(spark, start, end)
    loadData(spark, start, end)
    spark.stop()
  }
}