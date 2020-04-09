package utils

import java.util.TimeZone

import org.apache.spark.sql.SparkSession

object util {
  def main(args: Array[String]): Unit = {
    val spark = initSpark("kkk")
  }
  def initSpark(appName: String): SparkSession = {
    println(appName)
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[6]")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.default.parallelism", "18")
      .config("spark.cores.max", "6")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

}
