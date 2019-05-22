package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class TestCreate(val json:JsonObject) extends BaseWrappedJson {
    val test_id: String = getString("test_id")
    val test_name: String = getString("test_name")
    val test_version: String = getString("test_version")
}
