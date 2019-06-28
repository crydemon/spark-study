package com.vova.export

import java.io.File

import com.vova.export.Demand_1319.spark
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object Demand_1375

  extends App {
  //  val themisReader = new DataSource("themis_read")
  val appName = "d_1319"
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val orderInfo = spark
    .read
    .option("header", "true")
    .csv("d:/order_info.csv")
    .withColumn("event_date", functions.to_date($"pay_time"))

  val users = spark
    .read
    .option("header", "true")
    .csv("d:/users.csv")
    .withColumn("event_date", $"date".substr(0, 10))


  FileUtils.deleteDirectory(new File("d:/result"))
  val allUv = users
    .groupBy("event_date")
    .agg(
      functions.count(functions.lit(1)).alias("all_uv")
    )

  val frUv = users
    .filter($"country" === "FR")
    .groupBy("event_date")
    .agg(
      functions.count(functions.lit(1)).alias("fr_uv")
    )

  val gbUv = users
    .filter($"country" === "GB")
    .groupBy("event_date")
    .agg(
      functions.count(functions.lit(1)).alias("gb_uv")
    )


  val allOrdered = users
    .join(orderInfo, $"user_id" === $"user_unique_Id")
    .cache()
    .groupBy(orderInfo("event_date"))
    .agg(
      functions.count(functions.lit(1)).alias("all_payed_user"),
      functions.sum($"order_gmv").alias("all_payed_gmv")
    )


  val frOrdered = users
    .join(orderInfo, $"user_id" === $"user_unique_Id")
    .filter($"country" === "FR")
    .groupBy(orderInfo("event_date"))
    .agg(
      functions.count(functions.lit(1)).alias("fr_payed_user"),
      functions.sum($"order_gmv").alias("fr_payed_gmv")
    )

  val gbOrdered = users
    .join(orderInfo, $"user_id" === $"user_unique_Id")
    .filter($"country" === "GB")
    .groupBy(orderInfo("event_date"))
    .agg(
      functions.count(functions.lit(1)).alias("gb_payed_user"),
      functions.sum($"order_gmv").alias("gb_payed_gmv")
    )


  allUv
    .join(frUv, "event_date")
    .join(gbUv, "event_date")
    .join(allOrdered, "event_date")
    .join(frOrdered, "event_date")
    .join(gbOrdered, "event_date")
    .coalesce(1)
    .write
    .option("header", "true")
    .mode(SaveMode.Append)
    .csv("d:/result")


}
