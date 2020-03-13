package spark.ch2

import org.apache.spark.sql.SparkSession

object SparkSessionCreate {
  def createSession(appName:String): SparkSession = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "E:/Exp/")
      .appName(appName)
      .getOrCreate()

    return spark
  }
}