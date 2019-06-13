import com.vova.utils.S3Config
import org.apache.spark.sql.SparkSession

object S3Test extends App {
  val appName = "s3_export"
  println(appName)
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", S3Config.keyId)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", S3Config.accessKey)

  import spark.implicits._


  val firstOrderInfo = spark.read.parquet("D:\\first_order_info")
    .select("device_id", "is_new_user")
    .collect()
    .map(r => (r.getString(0), r.getLong(1)))
    .toMap


  println(firstOrderInfo.contains("dsfjs"))
  spark.stop()


}

case class FirstOrder(device_id: String, is_new_user: String)