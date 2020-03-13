package spark

object dependence extends App {

  val spark = SparkUtils.initSpark("kk")

  import spark.implicits._

  val df1 = Seq(23, 423, 4, 432, 432, 3, 6, 7).toDF("key")
  val df2 = Seq(23, 423, 4, 432, 432, 3, 6, 7, 3, 4, 5, 6, 5).toDF("key")
  df1.join(df2, "key")
    .groupBy("key")
    .count()
    .toJavaRDD.dependencies.foreach { dep =>
    println("dependency type:" + dep.getClass)
    println("dependency RDD:" + dep.rdd)
    println("dependency partitions:" + dep.rdd.partitions)
    println("dependency partitions size:" + dep.rdd.partitions.length)
  }
}
