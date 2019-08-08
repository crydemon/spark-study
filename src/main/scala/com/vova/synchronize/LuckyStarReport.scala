package com.vova.synchronize

import com.vova.conf.Conf
import com.vova.db.DataSource
import com.vova.export.DruidSQL.writeToCSV
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => F}

object LuckyStarReport {
  def update(spark: SparkSession, startTime: String, endTime: String): Unit = {
    val clickPush =
      s"""
         |SELECT amp.push_time,
         |       aelmp.event_time,
         |       amp.user_id,
         |       amp.event_type
         |FROM app_message_push amp
         |         INNER JOIN app_event_log_message_push aelmp ON amp.id = aelmp.event_value
         |WHERE amp.push_result = 2
         |  AND amp.event_type IN (400, 410, 411, 412, 413, 414)
         |  AND amp.push_time >= '$startTime'
         |  AND amp.push_time < '$endTime'
         |  AND aelmp.event_type = 'click'
         |  AND aelmp.event_time >= '$startTime'
         |  AND aelmp.event_time < '$endTime'
         |  AND aelmp.event_time >= amp.push_time
       """.stripMargin

    val tryPush =
      s"""
         |SELECT amp.push_time,
         |       amp.user_id,
         |       amp.event_type
         |FROM app_message_push amp
         |WHERE amp.push_result = 2
         |  AND amp.event_type IN (400, 410, 411, 412, 413, 414)
         |  AND amp.push_time >= '$startTime'
         |  AND amp.push_time < '$endTime'
      """.stripMargin

    val newOrder =
      s"""
         |SELECT loi.order_amount AS order_gmv,
         |       loi.order_amount - loi.bonus AS grand_total,
         |       loi.create_time  AS order_time,
         |       loi.user_id,
         |       loi.pay_status,
         |       loi.pay_time
         |FROM luckystar_order_info loi
         |WHERE loi.create_time >= '$startTime'
         | AND loi.create_time < '$endTime'
      """.stripMargin

    val oldOrder =
      s"""
         |SELECT oi.order_amount AS order_gmv,
         |       oi.goods_amount + oi.shipping_fee AS grand_total,
         |       oi.order_time,
         |       oi.user_id,
         |       oi.pay_status,
         |       oi.pay_time
         |FROM order_info oi
         |WHERE oi.order_time >= '$startTime'
         | AND oi.order_time < '$endTime'
         | AND  exists(select * from order_extension oe where oe.order_id = oi.order_id and oe.ext_name = 'luckystar_activity_id')
      """.stripMargin

    val reportDb = new DataSource("themis_report_read")
    val basePath = Conf.getString("s3.etl.primitive.tmp")
    reportDb
      .load(spark, clickPush)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .parquet(basePath + "click_push/")

    reportDb.load(spark, tryPush)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .parquet(basePath + "try_push/")

    val themisDb = new DataSource("themis_read")
    themisDb.load(spark, newOrder)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(basePath + "order_info/")

    themisDb.load(spark, oldOrder)
      .write
      .mode(SaveMode.Append)
      .parquet(basePath + "order_info/")

    import spark.implicits._

    spark.read
      .option("header", "true")
      .parquet(basePath + "click_push/")
      .withColumn("push_time", F.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .withColumn("event_time", F.regexp_replace($"event_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("click_push")

    spark.read
      .option("header", "true")
      .parquet(basePath + "try_push/")
      .withColumn("push_time", F.regexp_replace($"push_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("try_push")


    spark.read
      .option("header", "true")
      .parquet(basePath + "order_info/")
      .withColumn("order_time", F.regexp_replace($"order_time".substr(0, 19), "T", " "))
      .withColumn("pay_time", F.regexp_replace($"pay_time".substr(0, 19), "T", " "))
      .createOrReplaceTempView("order_info")


    val tryOrderDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as try_order_user
        |from try_push tp
        | inner join order_info oi using(user_id)
        |where to_unix_timestamp(order_time) > to_unix_timestamp(push_time)
        | and to_unix_timestamp(order_time) - to_unix_timestamp(push_time) < 3600 * 24
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()


    val tryPayDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as try_pay_user,
        | sum(order_gmv) as try_pay_gmv,
        | sum(grand_total) as try_grand_total
        |from (select first(push_time) as push_time, event_type, user_id from try_push group by date(push_time), event_type, user_id) tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(pay_time) > to_unix_timestamp(push_time)
        | and to_unix_timestamp(pay_time) - to_unix_timestamp(push_time) < 3600 * 24
        | and pay_status >=1
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()

    tryPayDf
      .show(false)


    val clickOrderDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as click_order_user
        |from click_push tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(order_time) > to_unix_timestamp(event_time)
        | and to_unix_timestamp(order_time) - to_unix_timestamp(event_time) < 3600 * 24
        |group by date(push_time), event_type
      """.stripMargin)
      .cache()

    clickOrderDf
      .show(false)

    val clickPayDf = spark.sqlContext.sql(
      """
        |select
        | date(push_time) as push_date,
        | event_type,
        | count(distinct user_id) as click_pay_user,
        | sum(order_gmv) as click_pay_gmv,
        | sum(grand_total) as click_grand_total
        |from
        | (select
        |   first(push_time) as push_time, first(event_time) as event_time, event_type, user_id
        |   from click_push
        |   group by date(push_time), event_type, user_id) tp
        | inner join order_info oi using(user_id)
        |where
        | to_unix_timestamp(pay_time) > to_unix_timestamp(event_time)
        | and to_unix_timestamp(pay_time) - to_unix_timestamp(event_time) < 3600 * 24
        | and pay_status >=1
        |group by date(push_time), event_type
      """.stripMargin)
    val data = tryOrderDf
      .join(tryPayDf, Seq("push_date", "event_type"))
      .join(clickOrderDf, Seq("push_date", "event_type"))
      .join(clickPayDf, Seq("push_date", "event_type"))
      .cache()

    clickPayDf
      .show(false)

    data.show(false)
    writeToCSV(data)
  }
}
