package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class CommonClick(val json:JsonObject) extends BaseWrappedJson {
    lazy val element_name: String = getString("element_name")
    lazy val element_type: String = getString("element_type")
    lazy val element_url: String = getString("element_url")
    lazy val element_id: String = getString("element_id")
    lazy val element_content: String = getString("element_content")
    lazy val list_uri: String = getString("list_uri")
    lazy val list_name: String = getString("list_name")
    lazy val picture: String = getString("picture")
    lazy val element_position: String = getString("element_position")
}
