package com.vova.export

import java.io.File

import com.vova.db.DataSource
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object Demand_1185 extends App {

  val appName = "Demand_1185"
  println(appName)
  val spark = SparkSession.builder
    .appName(appName)
    .master("local[4]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val savePath = "D:/result_1185"
  FileUtils.deleteDirectory(new File(savePath))
  val themisReader = new DataSource("themis_read")

  val payedSqlO =
    """
      |SELECT oi.pay_time,
      |       date(oi.pay_time) as event_date,
      |       og.goods_number,
      |       oi.order_id,
      |       oi.user_id,
      |       lg.activity_id,
      |       virtual_goods_id,
      |       og.shop_price + og.shipping_fee AS order_goods_gmv
      |FROM order_info oi
      |         INNER JOIN luckystar_group_member lgm USING (order_id)
      |         INNER JOIN order_goods og USING (order_id)
      |         INNER JOIN virtual_goods vg USING (goods_id)
      |         INNER JOIN luckystar_group lg USING (group_id)
      |WHERE oi.pay_time >= '2019-05-11'
      |  AND oi.pay_time < '2019-05-19'
      |  AND oi.pay_status >= 1
      |  AND lgm.luckystar_order_id = 0
    """.stripMargin

  val payedSqlN =
    """
      |SELECT pay_time,
      |       date(loi.pay_time) as event_date,
      |       lg.activity_id,
      |       virtual_goods_id,
      |       loi.user_id,
      |       luckystar_order_id as order_id,
      |       loi.order_amount AS order_goods_gmv
      |FROM luckystar_order_info loi
      |         INNER JOIN luckystar_group_member lgm USING (luckystar_order_id)
      |         INNER JOIN virtual_goods vg USING (goods_id)
      |         INNER JOIN luckystar_group lg USING (group_id)
      |WHERE pay_time >= '2019-05-11'
      |  AND pay_time < '2019-05-19'
      |  AND loi.pay_status >= 1
    """.stripMargin


  val orderedSqlO =
    """
      |SELECT oi.order_time,
      |       date(oi.order_time) as event_date,
      |       og.goods_number,
      |       oi.order_id,
      |       oi.user_id,
      |       lg.activity_id,
      |       virtual_goods_id
      |FROM order_info oi
      |         INNER JOIN luckystar_group_member lgm USING (order_id)
      |         INNER JOIN order_goods og USING (order_id)
      |         INNER JOIN virtual_goods vg USING (goods_id)
      |         INNER JOIN luckystar_group lg USING (group_id)
      |WHERE oi.order_time >= '2019-05-11'
      |  AND oi.order_time < '2019-05-19'
      |  AND lgm.luckystar_order_id = 0
    """.stripMargin


  val orderedSqlN =
    """
      |SELECT loi.create_time as order_time,
      |       loi.goods_id,
      |       date(loi.create_time) as event_date,
      |       lg.activity_id,
      |       loi.user_id,
      |       virtual_goods_id,
      |       luckystar_order_id as order_id
      |FROM luckystar_order_info loi
      |         INNER JOIN luckystar_group_member lgm USING (luckystar_order_id)
      |         INNER JOIN virtual_goods vg USING (goods_id)
      |         INNER JOIN luckystar_group lg USING (group_id)
      |WHERE loi.create_time >= '2019-05-11'
      |  AND loi.create_time < '2019-05-19'
    """.stripMargin


  val payedInfoO = themisReader.load(spark, payedSqlO)

  payedInfoO.createOrReplaceTempView("po")

  val po = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as payed_order_o,
      | sum(order_goods_gmv) as gmv_o
      |from po
      |group by event_date, virtual_goods_id
    """.stripMargin)

  spark.sql(
    """
      | select
      |  *
      | from (
      |  select
      |    event_date,
      |    virtual_goods_id,
      |    order_id,
      |    order_goods_gmv,
      |    row_number() over (partition by activity_id, user_id order by pay_time)  as rank
      |  from po
      | ) as tmp
      | where tmp.rank = 1
    """.stripMargin)
    .createOrReplaceTempView("poa")


  val poa = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as payed_order_o,
      | sum(order_goods_gmv) as gmv_o
      |from poa
      |group by event_date, virtual_goods_id
    """.stripMargin)

  val payedInfoN = themisReader.load(spark, payedSqlN)

  payedInfoN.createOrReplaceTempView("pn")

  val pn = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as payed_order_n,
      | sum(order_goods_gmv) as gmv_n
      |from pn
      |group by event_date, virtual_goods_id
    """.stripMargin)

  spark.sql(
    """
      | select
      |  *
      | from (
      |  select
      |    event_date,
      |    virtual_goods_id,
      |    order_id,
      |    order_goods_gmv,
      |    row_number() over (partition by activity_id, user_id order by pay_time)  as rank
      |  from pn
      | ) as tmp
      | where tmp.rank = 1
    """.stripMargin)
    .createOrReplaceTempView("pna")

  val pna = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as payed_order_n,
      | sum(order_goods_gmv) as gmv_n
      |from pna
      |group by event_date, virtual_goods_id
    """.stripMargin)

  val payedAll = po.join(pn, Seq("event_date", "virtual_goods_id"), "outer")
    .na.fill(0.0)
    .withColumn("payed_order_all",$"payed_order_o" + $"payed_order_n")
    .withColumn("gmv_all", $"gmv_o" + $"gmv_n")
    .select("event_date", "virtual_goods_id", "payed_order_all", "gmv_all")

  val payedA = poa.join(pna, Seq("event_date", "virtual_goods_id"), "outer")
    .na.fill(0.0)
    .withColumn("payed_order_first",$"payed_order_o" + $"payed_order_n")
    .withColumn("gmv_first", $"gmv_o" + $"gmv_n")
    .select("event_date", "virtual_goods_id", "payed_order_first", "gmv_first")

  val payed = payedAll.join(payedA, Seq("event_date", "virtual_goods_id"), "left")

  val orderedInfoO = themisReader.load(spark, orderedSqlO)
  orderedInfoO.createOrReplaceTempView("oo")
  val oo = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as ordered_order_o
      |from oo
      |group by event_date, virtual_goods_id
    """.stripMargin)

  spark.sql(
    """
      | select
      |  *
      | from (
      |  select
      |    event_date,
      |    virtual_goods_id,
      |    order_id,
      |    row_number() over (partition by activity_id, user_id order by order_time)  as rank
      |  from oo
      | ) as tmp
      | where tmp.rank = 1
    """.stripMargin)
    .createOrReplaceTempView("ooa")

  val ooa = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as ordered_order_o
      |from ooa
      |group by event_date, virtual_goods_id
    """.stripMargin)

  val orderedInfoN = themisReader.load(spark, orderedSqlN)
  orderedInfoN.createOrReplaceTempView("on")
  val on = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as ordered_order_n
      |from on
      |group by event_date, virtual_goods_id
    """.stripMargin)


  spark.sql(
    """
      | select
      |  *
      | from (
      |  select
      |    event_date,
      |    virtual_goods_id,
      |    order_id,
      |    row_number() over (partition by activity_id, user_id order by order_time)  as rank
      |  from on
      | ) as tmp
      | where tmp.rank = 1
    """.stripMargin)
    .createOrReplaceTempView("ona")


  val ona = spark.sql(
    """
      |select
      | event_date,
      | virtual_goods_id,
      | count(distinct order_id) as ordered_order_n
      |from ona
      |group by event_date, virtual_goods_id
    """.stripMargin)

  val orderedAll = oo.join(on, Seq("event_date", "virtual_goods_id"), "outer")
    .na.fill(0.0)
    .withColumn("ordered_order_all", $"ordered_order_n" + $"ordered_order_o" )
    .select("event_date", "virtual_goods_id", "ordered_order_all")


  val orderedA = ooa.join(ona, Seq("event_date", "virtual_goods_id"), "outer")
    .na.fill(0.0)
    .withColumn("ordered_order_first", $"ordered_order_n" + $"ordered_order_o" )
    .select("event_date", "virtual_goods_id", "ordered_order_first")

  val ordered = orderedAll.join(orderedA, Seq("event_date", "virtual_goods_id"), "left")

  payed.cache()
  ordered.cache()

  ordered
    .join(payed, Seq("event_date", "virtual_goods_id"), "outer")
    .coalesce(1)
    .write
    .mode(SaveMode.Append)
    .option("header", "true")
    .option("delimiter", ",")
    .csv(savePath)

}
