package spark.ch1

import org.apache.spark.sql.SparkSession
import spark.ch1.Preproessing.spark

object SparkSessionCreate {
  def createSession(): SparkSession = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "E:/Exp/")
      .appName(s"OneVsRestExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
  }
}
