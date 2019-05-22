package com.vova.snowplow.schema.cx

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class Mobile(val json:JsonObject) extends BaseWrappedJson {
    val os_type: String = getString("osType")
    lazy val os_version: String = getString("osVersion")
    lazy val carrier: String = getString("carrier")
    lazy val network_type: String = getString("networkType")

    lazy val idfa: String = getString("appleIdfa")
}
