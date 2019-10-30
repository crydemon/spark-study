import org.apache.spark.sql.SparkSession

object testPairRDD extends App {

    val spark = SparkSession
        .builder()
        .appName("java pair test")
        .master("local[4]")
        .getOrCreate()
    var rdd1 = spark.sparkContext.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    val result1 = rdd1.combineByKey(
        (v: Int) => {
            println( v + "_")
            v + "_"
        },
        (c: String, v: Int) => {
            println(c + "@" + v)
            c + "@" + v
        },
        (c1: String, c2: String) => {
            println( c1 + "$" + c2)
            c1 + "$" + c2
        }
    ).collect
    result1.foreach(println)
}
