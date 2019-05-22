package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class GoodsImpression(val json:JsonObject) extends BaseWrappedJson {
    val impressions:Seq[_GoodsImpressionBlock] = {
        val seq = getArray("impressions")
        if (seq != null) seq.map(x => new _GoodsImpressionBlock(x.asObject.orNull)) else Seq()
    }
}

class _GoodsImpressionBlock(val json:JsonObject) extends BaseWrappedJson {
    val list_uri:String = getString("list_uri")
    val list_type:String = getString("list_type")
    val page_size:Long = getLong("page_size")
    val page_no:Long = getLong("page_no")
    val product_list:Seq[_GoodsImpressionUnit] = {
        val seq = getArray("product_list")
        if (seq != null) seq.map(x => new _GoodsImpressionUnit(x.asObject.orNull)) else Seq()
    }
}

class _GoodsImpressionUnit(val json:JsonObject) extends BaseWrappedJson {
    val page_position:Long = getLong("page_position", -1)
    val absolute_position:Long = getLong("absolute_position", -1)
    val goods_id:Long = getLong("id", -1)
    val goods_sn:String = getString("goods_sn")
}
