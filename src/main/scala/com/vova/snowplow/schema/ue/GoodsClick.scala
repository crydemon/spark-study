package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class GoodsClick(val json:JsonObject) extends BaseWrappedJson {
    lazy val list_uri:String = getString("list_uri")
    lazy val list_type:String = getString("list_type")
    lazy val page_size:Long = getLong("page_size")
    lazy val page_no:Long = getLong("page_no")
    lazy val page_position:Long = getLong("page_position", -1)
    lazy val absolute_position:Long = getLong("absolute_position", -1)
    lazy val goods_id:Long = getLong("id", -1)
    lazy val goods_sn:String = getString("goods_sn")

    lazy val element_url:String = getString("element_url")
}
