import java.io.File
import java.time.LocalDateTime

import com.vova.db.DataSource
import com.vova.utils.S3Config
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object S3Test extends App {
  val appName = "s3_export"
  println(appName)
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[6]")
    .config("spark.executor.cores", 2)
    .config("spark.sql.shuffle.partitions", 30)
    .config("spark.default.parallelism", 18)
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //  spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
  //  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", S3Config.keyId)
  //  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", S3Config.accessKey)

  import spark.implicits._

  demand_1413(spark)
  def demand_1413(spark: SparkSession): Unit = {
    val tryOrder =
      """
        |SELECT date(amp.push_time)        AS push_date,
        |       amp.event_type,
        |       count(DISTINCT oi.user_id) AS try_order_user
        |FROM app_message_push amp
        |         INNER JOIN order_info oi USING (user_id)
        |WHERE amp.push_result = 2
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND oi.order_time > amp.push_time
        |  AND amp.push_time >= '2019-05-24'
        |  AND TIMESTAMPDIFF(HOUR, amp.push_time, oi.order_time) < 24
        |GROUP BY push_date, event_type
        |
     """.stripMargin

    val tryPay =
      """
        |SELECT date(amp.push_time)                    AS push_date,
        |       amp.event_type,
        |       count(DISTINCT oi.user_id)             AS try_pay_user,
        |       sum(oi.goods_amount + oi.shipping_fee) AS try_pay_gmv
        |FROM app_message_push amp
        |         INNER JOIN order_info oi USING (user_id)
        |WHERE amp.push_result = 2
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND oi.order_time > amp.push_time
        |  AND amp.push_time >= '2019-05-24'
        |  AND oi.pay_time >= '2019-05-24'
        |  AND oi.pay_status >= 1
        |  AND TIMESTAMPDIFF(HOUR, amp.push_time, oi.pay_time) < 24
        |GROUP BY push_date, event_type
      """.stripMargin

    val clickOrder =
      """
        |SELECT date(amp.push_time)        AS push_date,
        |       amp.event_type,
        |       count(DISTINCT oi.user_id) AS click_order_user
        |FROM app_message_push amp
        |         INNER JOIN order_info oi USING (user_id)
        |         INNER JOIN app_event_log_message_push aelmp ON amp.id = aelmp.event_value
        |WHERE aelmp.event_type = 'click'
        |  AND amp.push_date >= '2019-05-24'
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND aelmp.event_time >= '2019-05-24'
        |  AND aelmp.event_time >= amp.push_time
        |  AND oi.order_time > aelmp.event_time
        |  AND TIMESTAMPDIFF(HOUR, aelmp.event_time, oi.order_time) < 24
        |GROUP BY push_date, amp.event_type
      """.stripMargin

    val clickPay =
      """
        |SELECT date(amp.push_time)                    AS push_date,
        |       amp.event_type,
        |       count(1)                               AS click_pay_user,
        |       sum(oi.shipping_fee + oi.goods_amount) AS click_pay_gmv
        |FROM app_message_push amp
        |         INNER JOIN order_info oi USING (user_id)
        |         INNER JOIN app_event_log_message_push aelmp ON amp.id = aelmp.event_value
        |WHERE 1
        |  AND aelmp.event_type = 'click'
        |  AND amp.push_date >= '2019-05-24'
        |  AND amp.event_type IN (400, 410, 411, 412, 413)
        |  AND aelmp.event_time >= '2019-06-05'
        |  AND aelmp.event_time >= amp.push_time
        |  AND oi.pay_time > aelmp.event_time
        |  AND oi.order_time > aelmp.event_time
        |  AND oi.pay_time >= '2019-05-24'
        |  AND oi.pay_status >= 1
        |  AND TIMESTAMPDIFF(HOUR, aelmp.event_time, oi.pay_time) < 24
        |GROUP BY push_date, amp.event_type
      """.stripMargin

    val reportDb = new DataSource("themis_report_read")
    val tryOrderDf = reportDb.load(spark, tryOrder)
    val tryPayDf = reportDb.load(spark, tryPay)
    val clickOrderDf = reportDb.load(spark, clickOrder)
    val clickPayDf = reportDb.load(spark, clickPay)
    val data = tryOrderDf
      .join(tryPayDf, Seq("push_date", "event_type"))
      .join(clickOrderDf, Seq("push_date", "event_type"))
      .join(clickPayDf, Seq("push_date", "event_type"))
    writeToCSV(data)
  }

  def writeToCSV(data: DataFrame): Unit = {
    val savePath = "d:/result"
    FileUtils.deleteDirectory(new File(savePath))
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .option("delimiter", ",")
      .csv(savePath)
  }

  spark.stop()


}

case class FirstOrder(device_id: String, is_new_user: String)