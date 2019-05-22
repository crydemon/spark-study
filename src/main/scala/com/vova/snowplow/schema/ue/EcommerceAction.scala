package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class EcommerceAction(val json:JsonObject) extends BaseWrappedJson {
    val action: String = getString("action")
}
