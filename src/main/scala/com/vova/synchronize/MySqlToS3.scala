package com.vova.synchronize

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.vova.conf.Conf
import com.vova.db.DataSource
import org.apache.spark.sql.{SaveMode, SparkSession}

object MySqlToS3 {

  def loadOrderInfo(startTime: String, endTime: String): String = {
    val sql =
      s"""
         |SELECT og.order_id,
         |       og.sku,
         |       og.sku_id,
         |       og.goods_id,
         |       og.goods_name,
         |       og.goods_sn,
         |       og.goods_number,
         |       og.goods_weight,
         |       og.market_price,
         |       og.shop_price,
         |       og.shop_price_amount,
         |       og.shipping_fee,
         |       og.shipping_discount,
         |       og.bonus,
         |       og.is_gift,
         |       og.goods_status,
         |       og.provider_id,
         |       og.invoice_num,
         |       og.return_points,
         |       og.return_bonus,
         |       og.addtional_shipping_fee,
         |       og.added_fee,
         |       og.rush_order_fee,
         |       og.rush_order_fee_exchange,
         |       og.styles,
         |       og.img_type,
         |       og.goods_gallery,
         |       og.goods_price_original,
         |       og.tracking_id,
         |       og.sku_order_status,
         |       og.sku_pay_status,
         |       og.sku_shipping_status,
         |       og.is_sale,
         |       og.parent_rec_id,
         |       og.order_goods_sn,
         |       og.mct_shop_price,
         |       og.mct_shop_price_amount,
         |       og.mct_shipping_fee,
         |       og.coupon_code,
         |       oi.party_id,
         |       oi.order_sn,
         |       oi.user_id,
         |       oi.order_time,
         |       oi.latest_pay_time,
         |       oi.order_status,
         |       oi.shipping_status,
         |       oi.pay_status,
         |       oi.consignee,
         |       oi.gender,
         |       oi.country,
         |       oi.province,
         |       oi.province_text,
         |       oi.city,
         |       oi.city_text,
         |       oi.district,
         |       oi.district_text,
         |       oi.address,
         |       oi.zipcode,
         |       oi.tel,
         |       oi.mobile,
         |       oi.email,
         |       oi.best_time,
         |       oi.sign_building,
         |       oi.postscript,
         |       oi.important_day,
         |       oi.sm_id,
         |       oi.shipping_id,
         |       oi.shipping_name,
         |       oi.payment_id,
         |       oi.payment_name,
         |       oi.how_oos,
         |       oi.how_surplus,
         |       oi.pack_name,
         |       oi.card_name,
         |       oi.card_message,
         |       oi.inv_payee,
         |       oi.inv_content,
         |       oi.inv_address,
         |       oi.inv_zipcode,
         |       oi.inv_phone,
         |       oi.goods_amount,
         |       oi.goods_amount_exchange,
         |       oi.shipping_fee as order_shipping_fee,
         |       oi.duty_fee,
         |       oi.insure_fee,
         |       oi.shipping_proxy_fee,
         |       oi.payment_fee,
         |       oi.pack_fee,
         |       oi.card_fee,
         |       oi.money_paid,
         |       oi.surplus,
         |       oi.integral,
         |       oi.integral_money,
         |       oi.bonus as order_bonus,
         |       oi.order_amount,
         |       oi.base_currency_id,
         |       oi.order_currency_id,
         |       oi.order_currency_symbol,
         |       oi.rate,
         |       oi.order_amount_exchange,
         |       oi.from_ad,
         |       oi.referer,
         |       oi.confirm_time,
         |       oi.pay_time,
         |       oi.receive_time,
         |       oi.shipping_time,
         |       oi.shipping_date_estimate,
         |       oi.shipping_carrier,
         |       oi.shipping_tracking_number,
         |       oi.pack_id,
         |       oi.card_id,
         |       oi.invoice_no,
         |       oi.extension_code,
         |       oi.extension_id,
         |       oi.to_buyer,
         |       oi.pay_note,
         |       oi.invoice_status,
         |       oi.carrier_bill_id,
         |       oi.receiving_time,
         |       oi.biaoju_store_id,
         |       oi.parent_order_id,
         |       oi.track_id,
         |       oi.ga_track_id,
         |       oi.real_paid,
         |       oi.real_shipping_fee,
         |       oi.is_shipping_fee_clear,
         |       oi.is_order_amount_clear,
         |       oi.is_ship_emailed,
         |       oi.proxy_amount,
         |       oi.pay_method,
         |       oi.is_back,
         |       oi.is_finance_clear,
         |       oi.finance_clear_type,
         |       oi.handle_time,
         |       oi.start_shipping_time,
         |       oi.end_shipping_time,
         |       oi.shortage_status,
         |       oi.is_shortage_await,
         |       oi.order_type_id,
         |       oi.special_type_id,
         |       oi.is_display,
         |       oi.misc_fee,
         |       oi.additional_amount,
         |       oi.distributor_id,
         |       oi.taobao_order_sn,
         |       oi.distribution_purchase_order_sn,
         |       oi.need_invoice,
         |       oi.facility_id,
         |       oi.language_id,
         |       oi.coupon_cat_id,
         |       oi.coupon_config_value,
         |       oi.coupon_config_coupon_type,
         |       oi.is_conversion,
         |       oi.from_domain,
         |       oi.project_name,
         |       oi.user_agent_id,
         |       oi.token,
         |       oi.payer_id,
         |       oi.last_update_time,
         |       oi.payment_fee_exchange,
         |       oi.mideast_info,
         |       vg.virtual_goods_id,
         |       g.brand_id,
         |       r.region_code,
         |       CASE
         |           WHEN c.depth = 1
         |               THEN c.cat_name
         |           WHEN c_pri.depth = 1
         |               THEN c_pri.cat_name
         |           WHEN c_ga.depth = 1
         |               THEN c_ga.cat_name
         |           END                AS first_class,
         |
        |       CASE
         |           WHEN c.depth = 2
         |               THEN c.cat_name
         |           WHEN c_pri.depth = 2
         |               THEN c_pri.cat_name
         |           WHEN c_ga.depth = 2
         |               THEN c_ga.cat_name
         |           END                AS second_class,
         |       (SELECT group_concat(oe.ext_value)
         |        FROM order_extension oe
         |        WHERE oe.order_id = oi.order_id
         |        GROUP BY oi.order_id) AS extension_name
         |FROM order_goods og
         |         INNER JOIN order_info oi USING (order_id)
         |         INNER JOIN region r ON r.region_id = oi.country
         |         INNER JOIN goods g USING (goods_id)
         |         LEFT JOIN category c ON g.cat_id = c.cat_id
         |         LEFT JOIN brand b ON g.brand_id = b.brand_id
         |         LEFT JOIN category c_pri ON c.parent_id = c_pri.cat_id
         |         LEFT JOIN category c_ga ON c_pri.parent_id = c_ga.cat_id
         |         LEFT JOIN virtual_goods vg ON vg.goods_id = g.goods_id
         |WHERE oi.last_update_time >= '$startTime'
         | AND oi.last_update_time < '$endTime'
      """.stripMargin

    sql
  }

  def initSpark: SparkSession = SparkSession.builder
    .appName("MySqlToS3")
    .master("local[4]")
    .config("spark.executor.cores", 2)
    .config("spark.sql.shuffle.partitions", 30)
    .config("spark.default.parallelism", 18)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    var (start, end) = (LocalDate.parse(args(0), dateFormat), LocalDate.parse(args(1), dateFormat))
    val themisDb = new DataSource("themis_read")
    val MySQLDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val spark = initSpark
    while (start.compareTo(end) <= 0) {
      val startTime = start.format(MySQLDateFormat)
      println(startTime)
      val nextDay = start.plusDays(1)
      val endTime = nextDay.format(MySQLDateFormat)
      themisDb
        .load(spark, loadOrderInfo(startTime, endTime))
        .write
        .mode(SaveMode.Overwrite)
        .parquet(Conf.getString("s3.etl.primitive.order_info") + startTime + "/")
      start = nextDay
    }
  }

}
