package com.vova.export

import java.io.File

import com.vova.db.DataSource
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import scala.collection.JavaConversions._

object Demand_1319 extends App {
  //  val themisReader = new DataSource("themis_read")
  val appName = "d_1319"
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._


  def query(in: String): String = {
    s"""
       |SELECT order_goods_sn,
       |       ost.last_update_time
       |FROM order_goods og
       |         INNER JOIN  sku_ops_log ost ON ost.order_goods_id = og.rec_id
       |where ops='sku_shipping_status' and status = 2 and worker != 'webhook'
       |    AND og.order_goods_sn IN($in)
    """.stripMargin
  }

  val orderInfo = spark
    .read
    .option("header", "true")
    .csv("d:/order_info.csv")


  val pushInfo = spark
    .read
    .option("header", "true")
    .csv("d:/push_info.csv")
    .groupBy("user_id", "event_type")
    .agg(
      functions.first("push_time").alias("push_time"),
      functions.first("event_time").alias("event_time")
    )

  val ordered = orderInfo
    .join(pushInfo, "user_id")
    .withColumn("push_date", functions.to_date($"push_time"))
    .withColumn("hour_diff", functions.hour(functions.to_timestamp($"create_time")) - functions.hour(functions.to_timestamp($"push_time")))
    .withColumn("hour_diff_click", functions.hour(functions.to_timestamp($"event_time")) - functions.hour(functions.to_timestamp($"push_time")))
    .cache()


  val payed = orderInfo
    .filter($"pay_status" >= 1)
    .join(pushInfo, "user_id")
    .withColumn("push_date", functions.to_date($"push_time"))
    .withColumn("hour_diff", functions.hour(functions.to_timestamp($"create_time")) - functions.hour(functions.to_timestamp($"push_time")))
    .withColumn("hour_diff_click", functions.hour(functions.to_timestamp($"event_time")) - functions.hour(functions.to_timestamp($"push_time")))
    .cache()


  val ordered_1 = ordered
    .filter($"hour_diff_click" >= 0 )
    .filter($"hour_diff_click" <= 1 )
    .groupBy("push_date", "event_type")
    .agg(
      functions.countDistinct("user_id").alias("1_ordered_user"),
      functions.count(functions.lit(1)).alias("1_ordered_num")
    )
  val ordered_24 = ordered
    .filter($"hour_diff_click" >= 0 )
    .filter($"hour_diff_click" <= 24)
    .groupBy("push_date", "event_type")
    .agg(
      functions.countDistinct("user_id").alias("24_ordered_user"),
      functions.count(functions.lit(1)).alias("24_ordered_num")
    )

  FileUtils.deleteDirectory(new File("d:/result"))


  val payed_1 = payed
    .filter($"hour_diff_click" >= 0 )
    .filter($"hour_diff_click" <= 1)
    .groupBy("push_date", "event_type")
    .agg(
      functions.countDistinct("user_id").alias("1_payed_user"),
      functions.sum("order_amount").alias("1_payed_amount")
    )


  val payed_24 = payed
    .filter($"hour_diff_click" >= 0 )
    .filter($"hour_diff_click" <= 24)
    .groupBy("push_date", "event_type")
    .agg(
      functions.countDistinct("user_id").alias("24_payed_user"),
      functions.sum("order_amount").alias("24_payed_amount")
    )
  ordered_24
    .join(ordered_1, Seq("push_date", "event_type"), "left")
    .join(payed_1, Seq("push_date", "event_type"), "left")
    .join(payed_24, Seq("push_date", "event_type"), "left")
    .coalesce(1)
    .write
    .option("header", "true")
    .mode(SaveMode.Append)
    .csv("d:/result")

}
