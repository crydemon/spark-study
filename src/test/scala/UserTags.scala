import com.vova.synchronize.UserTags
import org.apache.spark.sql.SparkSession

object UserTagsTest extends App {
  val appName = "user_tags"
  println(appName)
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  UserTags.updateUsersTagTable(spark, "2018-03-01", "2019-05-18")
  spark.stop()
}