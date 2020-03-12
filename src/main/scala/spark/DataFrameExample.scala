package spark

import org.apache.spark.sql.SparkSession
//https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md
object RedisExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val personSeq = Seq(Person("John", 30), Person("Peter", 45))
    val df = spark.createDataFrame(personSeq)

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .save()
    //By default Spark-Redis generates UUID identifier for each row to ensure their uniqueness.
    // However, you can also provide your own column as a key. This is controlled with key.column option:

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .option("key.column", "name")
      .save()
  }
}