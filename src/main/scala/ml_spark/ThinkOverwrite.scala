package ml_spark

import org.apache.spark.sql.{Row, functions}
import utils.util.initSpark

import scala.util.Try


object ThinkOverwrite extends App {

  val spark = initSpark("chapter2")

  import spark.implicits._

  def string2double(gmv: String): Double = {
    Try(gmv.toDouble).getOrElse(0.0)
  }


//  val df = Seq(
//    (1, 100, 234, "2019-10-18", "john", 23.2),
//    (2, 200, 234, "2019-11-19", "john", 213.2),
//    (2, 201, 214, "2018-12-17", "john", 21.25),
//    (2, 220, 234, "2017-11-15", "john", 123.2),
//    (3, 301, 234, "2011-08-14", "john", 233.2),
//    (3, 311, 234, "2011-07-28", "zoe", 24.25),
//    (2, 221, 234, "2019-10-18", "zoe", 13.23),
//    (3, 305, 234, "2019-11-19", "zoe", 233.3),
//    (4, 401, 434, "2018-12-17", "joy", 243.2),
//    (5, 501, 534, "2017-11-15", "joy", 243.2),
//    (7, 701, 734, "2011-08-14", "joy", 243.2)
//  ).toDF("cat_id", "goods_id", "pay_date", "user_id", "user_name", "gmv")
//
//  df.write
//    .mode("overwrite")
//    .option("header", "true")
//    .csv("D:\\datas\\test")

  //确实可以覆盖
  val df1 = spark.read.option("header", "true")
    .csv("D:\\datas\\test")
    .cache()
  println(df1.count())

  import spark.implicits._
  df1.rdd
    .map(x=>(x.getInt(0), 1))
    .reduceByKey((x, y)  => x+ y)
    .count()

  df1
    .select("goods_id")
    .groupBy("goods_id")
    .count()
    .show(false)
  //
  //  val df1 =  Seq(
  //    (234,  "2019-10-18", 1, "17.14.59.7"),
  //    (234,  "2019-11-19", 1, "17.114.9.7"),
  //    (236,  "2018-12-17", 1, "17.114.59.7"),
  //    (236,  "2017-11-15", 1, "17.14.159.27"),
  //    (278,  "2011-08-14", 1, "217.114.159.27"),
  //    (214,  "2011-07-28", 1, "217.114.159.7")
  //  ).toDF( "user_id", "login_date", "times", "ip")
  //  df.createTempView("oi")
  //  df1.createTempView("lg")
  //  //不支持
  //  println(df.rdd.getNumPartitions)
  //  println(df.rdd.getCheckpointFile)
  //  println(df.rdd.getStorageLevel)
  //  df.groupBy("cat_id").count().show(false)
  //
  //  df.repartition(20).show(false)
}
