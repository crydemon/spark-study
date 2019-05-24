import java.time.LocalDate
import java.time.format.DateTimeFormatter


import com.vova.export.Demand_1199.loadData
import com.vova.utils.S3Config
import org.apache.spark.sql.SparkSession

object Demand_1199_Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("s3")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.set("fs.s3.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3.access.key", S3Config.keyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3.secret.key", S3Config.accessKey)

    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start: LocalDate = LocalDate.parse("2019-03-01", dateFormat)
    val end: LocalDate = LocalDate.parse("2019-03-01", dateFormat)
    loadData(spark, start, end)
    spark.stop()
    spark.stop()
  }
}
