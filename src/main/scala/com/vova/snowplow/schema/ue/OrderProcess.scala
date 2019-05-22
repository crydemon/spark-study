package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class OrderProcess(val json:JsonObject) extends BaseWrappedJson {
    val element_name: String = getString("element_name")
    val submit_result: String = getString("submit_result")
    val goods_id: String = getString("goods_id")
    val payment_method: String = getString("payment_method")
}
