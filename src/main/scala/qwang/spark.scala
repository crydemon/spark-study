package qwang

import java.util.TimeZone

import org.apache.spark.sql.SparkSession

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

        println(spark.conf.get("spark.driver.extraJavaOptions"))
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

