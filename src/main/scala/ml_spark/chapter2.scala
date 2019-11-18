package ml_spark

import utils.util.initSpark


object chapter2 extends App {

  val spark = initSpark("chapter2")

  import spark.implicits._

  val df = Seq(
    (1, 100, 234, "john", 23.2),
    (2, 200, 234, "john", 213.2),
    (2, 201, 214, "john", 21.25),
    (2, 220, 234, "john", 123.2),
    (3, 301, 234, "john", 233.2),
    (3, 311, 234, "zoe", 24.25),
    (2, 221, 234, "zoe", 13.23),
    (3, 305, 234, "zoe", 233.3),
    (4, 401, 434, "joy", 243.2),
    (5, 501, 534, "joy", 243.2),
    (7, 701, 734, "joy", 243.2)
  ).toDF("cat_id", "goods_id", "user_id", "user_name", "gmv")
  println(df.rdd.getNumPartitions)
  println(df.rdd.getCheckpointFile)
  println(df.rdd.getStorageLevel)
  df.groupBy("cat_id").count().show(false)

  df.repartition(20).show(false)
  spark.read.option("header","true").csv("D:\\work\\report-v1\\src\\main\\scala\\ml_spark\\hit-daily-unrepl.csv").show(false)
}
