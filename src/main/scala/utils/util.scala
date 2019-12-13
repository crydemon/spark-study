package utils

import java.util.TimeZone

import org.apache.spark.sql.SparkSession

object util {
  def initSpark(appName: String): SparkSession = {
    println(appName)
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[6]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.default.parallelism", "18")
      .config("spark.cores.max", "6")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
