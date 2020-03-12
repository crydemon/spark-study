

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object GroupByTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("GroupBy Test")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("init end")
    val seq = Seq(('a', 1), ('b', 2), ('c', 3), ('c', 3), ('c', 3), ('c', 3), ('c', 3), ('c', 3))
    val rdd = spark.sparkContext.makeRDD(seq)
    println(rdd.getNumPartitions)
    val shuffleRdd =rdd.reduceByKey((x, y) => x + y)
    println(shuffleRdd.toDebugString)
    shuffleRdd.foreach(tup => println(tup._1 + "," + tup._2))


    val sc = spark.sparkContext
    val data = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)


    val data2 = sc.parallelize(List("a c", "a b", "b c", "b d", "c d"), 2)
    val wordcount2 = data2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).reduceByKey(_ + _)


    val rdd2 = wordcount.join(wordcount2)
    println(rdd2.toDebugString)
    spark.stop()

  }
}