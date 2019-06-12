package com.vova.synchronize

import java.text.SimpleDateFormat
import java.util.Calendar

import com.vova.db.DataSource
import org.apache.spark.sql.SparkSession

object UserTags {
  private val avg = 16.565135

  private val typeMap: Map[String, String] = Map(
    "device_id" -> "string",
    "user_id" -> "int",
    "country_code" -> "string",
    "platform" -> "string",
    "channel" -> "string",
    "activate_date" -> "string",
    "activate_time" -> "string",
    "first_payed_time" -> "string",
    "last_payed_time" -> "string",
    "acc_payed_amount" -> "decimal",
    "acc_payed_order" -> "long",
    "recent_two_intervals" -> "int",
    "M_tag" -> "string",
    "R_tag" -> "string",
    "F_tag" -> "string"
  )

  def devicePayed(startTime: String, endTime: String): String = {
    s"""
       |SELECT su.event_time as activate_time,
       |       su.device_id,
       |       date(su.event_time) as activate_date,
       |       su.uid as user_id,
       |       su.country                        AS country_code,
       |       ar.media_source AS channel,
       |       su.platform,
       |       oi.shipping_fee + oi.goods_amount AS order_gmv,
       |       oi.pay_time
       |FROM app_event_log_user_start_up su
       |         LEFT JOIN appsflyer_record ar USING (device_id)
       |         LEFT JOIN order_info oi ON oi.device_id = su.device_id AND oi.pay_status >= 1 AND oi.parent_order_id = 0
       |WHERE su.event_time >= '$startTime'
       |  AND su.event_time < '$endTime'
      """.stripMargin
  }

  def createUserTagsV2: String = {
    """
      |CREATE TABLE `user_tags_v2`
      |(
      |    `device_id`            varchar(64) NOT NULL DEFAULT '',
      |    `user_id`              int(11)     NOT NULL DEFAULT '0',
      |    `idfa`                 varchar(64),
      |    `idfv`                 varchar(64),
      |    `currency`             varchar(8),
      |    `advertising_id`       varchar(64),
      |    `bundle_id`            varchar(64),
      |    `country_code`         varchar(2)  NOT NULL DEFAULT '',
      |    `platform`             varchar(8)  NOT NULL DEFAULT '',
      |    `channel`              varchar(64)          DEFAULT '',
      |    `activate_date`        date        NOT NULL DEFAULT 0 COMMENT '首次激活日期',
      |    `activate_time`        timestamp   NOT NULL DEFAULT 0 COMMENT '首次激活时间',
      |    `first_payed_time`     timestamp            DEFAULT 0 COMMENT '首单支付时间',
      |    `last_payed_time`      timestamp            DEFAULT 0 COMMENT '最后一次支付时间',
      |    `acc_payed_amount`     decimal(10, 2)       DEFAULT '0.00' COMMENT '累计支付金额',
      |    `acc_payed_order`      int(11)              DEFAULT NULL COMMENT '累计支付订单数',
      |    `recent_two_intervals` int(11)              DEFAULT NULL COMMENT '最近两次支付间隔',
      |    `M_tag`                varchar(8)           DEFAULT NULL COMMENT '用户价值标签',
      |    `R_tag`                varchar(8)           DEFAULT NULL COMMENT '用户活跃程度标签',
      |    `F_tag`                varchar(8)           DEFAULT NULL COMMENT '用户消费频率标签',
      |    `create_time`          timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
      |    `last_update_time`     timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last update time',
      |    PRIMARY KEY `device_id` (`device_id`),
      |    KEY `user_id` (`user_id`),
      |    KEY `first_payed_time` (`first_payed_time`),
      |    KEY `acc_payed_amount` (`acc_payed_amount`),
      |    KEY `last_payed_time` (`last_payed_time`),
      |    KEY `recent_two_intervals` (`recent_two_intervals`),
      |    KEY `channel` (`channel`),
      |    KEY `country_code` (`country_code`),
      |    KEY `activate_date` (`activate_date`),
      |    KEY `create_time` (`create_time`)
      |) ENGINE = MyISAM
      |  DEFAULT CHARSET = utf8mb4 COMMENT ='用户分层';
    """.stripMargin
  }

  //全量更新激活时间在start-end的设备
  def updateUsersTagTable(spark: SparkSession, start: String, end: String) = {
    //所有设备的渠道,国家,激活日期
    println(avg)
    val pattern = "yyyy-MM-dd"
    val fmt = new SimpleDateFormat(pattern)
    val calendar = Calendar.getInstance()
    calendar.setTime(fmt.parse(start))
    val endDate = fmt.parse(end)

    val reportDb = new DataSource("themis_report_read")
    val reportDbWriter = new DataSource("themis_report_write")
    while (calendar.getTime.compareTo(endDate) <= 0) {

      val startTime = fmt.format(calendar.getTime)
      calendar.add(Calendar.DATE, 1)
      val endTime = fmt.format(calendar.getTime)

      println(startTime)

      val devicePayedSql = devicePayed(startTime, endTime)
      reportDb.load(spark, devicePayedSql).createOrReplaceTempView("order_info")


      spark
        .sql(
          s"""
             | select
             |   oi.device_id,
             |   first(channel) as channel,
             |   first(country_code) as country_code,
             |   first(activate_date) as activate_date,
             |   first(activate_time) as activate_time,
             |   first(user_id) as user_id,
             |   first(platform) as platform,
             |   min(oi.pay_time) as first_payed_time,
             |   max(oi.pay_time) as last_payed_time,
             |   sum(oi.order_gmv)  as acc_payed_amount,
             |   count(oi.pay_time) as acc_payed_order,
             |   count(oi.pay_time) as F_tag,
             |   case
             |     when count(oi.pay_time) = 0 then '(0-1/2]'
             |     when sum(oi.order_gmv) / count(1) <= 1/2 * ${avg} then '(0-1/2]'
             |     when sum(oi.order_gmv) / count(1) <= ${avg} then '(1/2-1]'
             |     when sum(oi.order_gmv) / count(1) <= 2 * ${avg} then '(1-2]'
             |     else '(2-]'
             |   end as M_tag
             | from order_info oi
             | group by device_id
          """.stripMargin)
        .createOrReplaceTempView("consume")

      val recent = spark
        .sql(
          s"""
             | select
             |  *
             | from
             | (select
             |   device_id,
             |   pay_time,
             |  row_number()
             |   over (partition by device_id order by pay_time desc)
             |     as rank
             | from order_info
             | ) as tmp
             | where tmp.rank = 2
            """.stripMargin)
      recent.createOrReplaceTempView("second")

      spark
        .sql(
          s"""
             | select
             |   datediff(to_date(c.last_payed_time), if(s.pay_time is null, to_date(c.last_payed_time), to_date(s.pay_time))) AS recent_two_intervals,
             |   case
             |     when s.pay_time is null then '[0-15]'
             |     when datediff(to_date(c.last_payed_time), to_date(s.pay_time)) <= 15 then '[0-15]'
             |     when datediff(to_date(c.last_payed_time), to_date(s.pay_time)) <= 30 then '(15-30]'
             |     when datediff(to_date(c.last_payed_time), to_date(s.pay_time)) <= 45 then '(30-45]'
             |     when datediff(to_date(c.last_payed_time), to_date(s.pay_time)) <= 60 then '(45-60]'
             |     else '(60-]'
             |   end as R_tag,
             |   c.device_id
             | from consume c
             |   left join second s using(device_id)
            """.stripMargin)
        .createOrReplaceTempView("activity")

      val data = spark
        .sql(
          s"""
             | select
             |   country_code,
             |   device_id,
             |   if(channel is null, '', channel) as channel,
             |   user_id,
             |   platform,
             |   if(activate_date is null, '0000-00-00', activate_date) as activate_date,
             |   if(activate_time is null, '0000-00-00 00:00:00', activate_time) as activate_time,
             |   if(first_payed_time is null, '0000-00-00 00:00:00', first_payed_time) as first_payed_time,
             |   if(last_payed_time is null, '0000-00-00 00:00:00', last_payed_time) as last_payed_time,
             |   if(acc_payed_amount is null, 0.0, acc_payed_amount) as acc_payed_amount,
             |   if(acc_payed_order is null, 0, acc_payed_order) as acc_payed_order,
             |   if(recent_two_intervals is null, 0, recent_two_intervals) as recent_two_intervals,
             |   if(M_tag is null, '', M_tag) as M_tag,
             |   if(R_tag is null, '', R_tag) as R_tag,
             |   if(F_tag is null, '', F_tag) as F_tag
             | from consume
             |   left join activity using (device_id)
          """.stripMargin)
      data.cache()
      //reportDbWriter.insert(typeMap, "user_tags", data)
      //data.show(truncate = false)
      reportDbWriter.insertPure("user_tags_v2", data, spark)
    }
  }

  def main(args: Array[String]): Unit = {

    val appName = "user_tags"
    println(appName)
    val spark = SparkSession.builder
      .master("yarn")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.yarn.maxAppAttempts", 1)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("s3://vomkt-emr-rec/checkpoint/")
    //    val pattern = "yyyy-MM-dd"
    //    val fmt = new SimpleDateFormat(pattern)
    //    val calendar = Calendar.getInstance()
    //    val end = fmt.format(calendar.getTime)
    //    calendar.add(Calendar.DATE, -6)
    //    val start = fmt.format(calendar.getTime)
    val reportDbWriter = new DataSource("themis_report_write")
    reportDbWriter.execute(createUserTagsV2)
    val start = "2018-04-24"
    val end = "2019-05-20"
    updateUsersTagTable(spark, start, end)

    spark.stop()
  }
}





