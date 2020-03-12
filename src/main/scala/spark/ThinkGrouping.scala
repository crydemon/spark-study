package spark

import org.apache.spark.sql.DataFrame

case class MemberOrderInfo(area: String, goodsId: String, memberType: String, product: String, price: Int)

object ThinkGrouping extends App {
  val spark = utils.util.initSpark("think_encoder")

  import spark.implicits._

  val orders = Seq(
    MemberOrderInfo("深圳", "432", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("深圳", "43", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("深圳", "34", "钻石会员", "钻石会员3个月", 70),
    MemberOrderInfo("深圳", "34", "钻石会员", "钻石会员12个月", 300),
    MemberOrderInfo("深圳", "34", "铂金会员", "铂金会员3个月", 60),
    MemberOrderInfo("深圳", "34", "铂金会员", "铂金会员3个月", 60),
    MemberOrderInfo("深圳", "34", "铂金会员", "铂金会员6个月", 120),
    MemberOrderInfo("深圳", "134", "黄金会员", "黄金会员1个月", 15),
    MemberOrderInfo("深圳", "34", "黄金会员", "黄金会员1个月", 15),
    MemberOrderInfo("深圳", "34", "黄金会员", "黄金会员3个月", 45),
    MemberOrderInfo("深圳", "34", "黄金会员", "黄金会员12个月", 180),
    MemberOrderInfo("北京", "3234", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("北京", "34", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("北京", "324", "铂金会员", "铂金会员3个月", 60),
    MemberOrderInfo("北京", "34", "黄金会员", "黄金会员3个月", 45),
    MemberOrderInfo("上海", "34", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("上海", "3424", "钻石会员", "钻石会员1个月", 25),
    MemberOrderInfo("上海", "34", "铂金会员", "铂金会员3个月", 60),
    MemberOrderInfo("上海", "3454", "黄金会员", "黄金会员3个月", 45)
  )
  //把seq转换成DataFrame
  val memberDF: DataFrame = orders.toDF()
  //把DataFrame注册成临时表
  memberDF.createOrReplaceTempView("orderTempTable")
  spark.sql(
    """
      |select area,memberType,product,sum(price) as total from orderTempTable group by area,memberType,product
      |""".stripMargin)
    .show(200, false)

  //  spark.sql(
  //    """
  //      |select area,memberType,product,sum(price) as total
  //      |from orderTempTable
  //      |group by area,memberType,product
  //      |grouping sets(area,memberType,product)
  //      |order by area,memberType,product
  //      |""".stripMargin)
  //    .show(200, false)

  spark.sql(
    """
      |select area,memberType,goodsId,product,sum(price) as total
      |from orderTempTable
      |group by area,memberType,product,goodsId
      |grouping sets(area,goodsId, (memberType,product), (area,memberType,product, goodsId))
      |order by area,memberType,product,goodsId
      |""".stripMargin)
    .show(200, false)

}
