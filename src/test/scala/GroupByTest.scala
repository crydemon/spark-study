import java.util.Random

import org.apache.spark.sql.SparkSession

/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [valSize] [numReducers]
  */
object GroupByTest {
  def main(args: Array[String]) {

    val numMappers = 100
    val numKVPairs = 10000
    val valSize = 1000
    val numReducers = 36

    val spark = SparkSession.builder()
      .appName("GroupBy Test")
      .master("local[4]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.default.parallelism", "18")
      .config("spark.cores.max", "6")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .getOrCreate()

    val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    pairs1.count

    println(pairs1.groupByKey(numReducers).count)


    println(pairs1.toDebugString)

    spark.stop()
    Thread.sleep(100000000)
  }
}