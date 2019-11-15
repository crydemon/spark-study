package qwang

import java.util.TimeZone

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}

object SparkUtils {

  def initSpark(appName: String): SparkSession = {
    println(appName)
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[4]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.default.parallelism", "18")
      .config("spark.cores.max", "6")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    spark.conf.set("spark.sql.broadcastTimeout", "36000")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

object SparkGroupByKey extends App {
  val words = Array("one", "two", "two", "three", "three", "three")
  val spark = SparkUtils.initSpark("groupByKey")
  val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))
  //group
  //This operation may be very expensive
  val wordCountsWithGroup = wordPairsRDD
    .groupByKey()
    .map(t => (t._1, t._2.sum))
    .collect()
  //reduce
  //reduceByKey 更适合使用在大数据集上
  val wordCountsWithReduce = wordPairsRDD
    .reduceByKey(_ + _)
    .collect()
  Thread.sleep(1000000)
}

object Partition extends App {
  val words = Array("one", "two", "two", "three", "three", "three")
  val spark = SparkUtils.initSpark("groupByKey")
  val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))

  wordPairsRDD
    .partitionBy(new HashPartitioner(2))
    .reduceByKey(_ + _)
    .collect()


  Thread.sleep(1000000)
}

object ZipOp extends App {
  val spark = SparkUtils.initSpark("groupByKey")

  import spark.implicits._

  //会 deserializer
  val radd1 = spark.range(1, 4, 1).rdd
  val radd2 = spark.range(4, 7, 1).rdd
  val radd3 = spark.range(7, 10, 1).rdd


  (radd1 zip radd2 zip radd3).toDF()
  //.show(false)
  radd3.zipWithIndex().toDF()
  //.show(false)
  spark.range(1, 10, 2).rdd
    .zipWithUniqueId().toDF()
  //.show(false)
  spark.sparkContext.makeRDD(Array((1, "A"), (2, "B"), (3, "C"), (4, "D")), 2)
    .mapValues(_ + "_")
    .map(_ + "")
  //.foreach(println)
  //foldByKey和reduceByKey的功能是相似的，都是在map端先进行聚合，再到reduce聚合。不同的是flodByKey需要传入一个参数。该参数是计算的初始值。
  spark.sparkContext
    .makeRDD(Array(("A", 0), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    .foldByKey(2)(_ + _)
    .foreach(println)

  /**
    * 参考：
    * https://www.jianshu.com/p/d7552ea4f882
    * https://cloud.tencent.com/developer/ask/98711
    * 该函数用于将RDD[K,V]转换成RDD[K,C],这里的V类型和C类型可以相同也可以不同。
    *
    * def combineByKey[C](
    * createCombiner: V => C,
    * mergeValue: (C, V) => C,
    * mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
    * combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
    * }
    *
    * 参数的含义如下：
    * createCombiner：组合器函数，用于将V类型转换成C类型，输入参数为RDD[K,V]中的V,输出为C
    * mergeValue：在每个分区上执行;合并值函数，将一个C类型和一个V类型值合并成一个C类型，输入参数为(C,V)，输出为C,
    * mergeCombiners：将不同分区的结果合并;合并组合器函数，用于将两个C类型值合并成一个C类型，输入参数为(C,C)，输出为C
    * numPartitions：结果RDD分区数，默认保持原有的分区数
    * partitioner：分区函数,默认为HashPartitioner
    * mapSideCombine：是否需要在Map端进行combine操作，类似于MapReduce中的combine，默认为true
    * serializer：序列化类，默认为null
    */
  // 对各个科目求平均值
  val scores = spark.sparkContext.makeRDD(List(("chinese", 88), ("chinese", 90), ("math", 60), ("math", 87)), 2)
  var avgScoresRdd = scores.combineByKey(
    (x: Int) => (x, 1),
    (c: (Int, Int), x: Int) => (c._1 + x, c._2 + 1),
    (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2))
  spark.createDataFrame(avgScoresRdd).show()
  var avgScores = avgScoresRdd.map { case (key, value) => (key, value._1 / value._2.toFloat) } //.map(x=>(x,(x._1/x._2))
  Thread.sleep(1000000)
}

object WindowTest extends App {
  val spark = SparkUtils.initSpark("windowTest")

  import spark.implicits._

  val sampleData = Seq(("bob", "Developer", 0), ("bob", "Developer", 125002), ("bob", "Developer", 12002), ("mark", "Developer", 108000), ("carl", "Tester", 70000), ("peter", "Developer", 185000), ("jon", "Tester", 65000), ("roman", "Tester", 82000), ("simon", "Developer", 98000), ("eric", "Developer", 144000), ("carlos", "Tester", 75000), ("henry", "Developer", 110000)).toDF("Name", "Role", "Salary")

  sampleData.orderBy("Name", "Role").show(false)
  val window = Window.partitionBy("Name", "Role").orderBy("Salary")
  sampleData
    .withColumn("first_val", F.first("Salary", true).over(window))
    .withColumn("last_val", F.last("Salary", true).over(window))
    .withColumn("col_last_val", F.coalesce($"Salary", F.last("Salary", true).over(window)))
    .orderBy("Name", "Role")
    .show(false)
}
