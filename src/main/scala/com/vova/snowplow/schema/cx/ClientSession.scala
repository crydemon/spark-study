package com.vova.snowplow.schema.cx

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class ClientSession(val json:JsonObject) extends BaseWrappedJson {
    val session_id: String = getString("sessionId")
}
