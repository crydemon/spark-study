import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Base64

import com.google.gson.{Gson, JsonArray}
import com.vova.export.Demand_1199.loadData
import com.vova.utils.S3Config
import org.apache.avro.data.Json
import org.apache.spark.sql.{SparkSession, functions}

import scala.util.matching.Regex

case class badEvent(error: String, info: String)

object search_bad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("s3")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = spark.read.json("E:\\eks\\vomkt-evt\\enrich-bad\\2019\\06\\01\\06")

    val data2 = data
      .selectExpr("explode(errors) as e", "line")
      .withColumn("error", functions.substring_index($"e.message", "\n", 1))
      .select("error", "line")
      .map(row => {
        val error = row.getString(0)
        val line = row.getString(1)
        val pattern = new Regex("\"data\":(\\[[^][]+])")
        pattern findAllIn (line)
        val info = new String(Base64.getDecoder.decode(row.getString(1)), "UTF-8")

        badEvent(error, info)
      })


      .cache()
    data2.printSchema()
    data2.show(1, truncate = false)
    spark.stop()
  }
}
