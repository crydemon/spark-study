package com.vova.snowplow.schema.cx

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class AppCommon(val json:JsonObject) extends BaseWrappedJson {
    lazy val app_version: String = getString("app_version")
    lazy val device_model: String = getString("device_model")
    lazy val device_id: String = getString("device_id")
    lazy val imei: String = getString("imei")
    lazy val android_id: String = getString("android_id")
    lazy val idfv: String = getString("idfv")
    lazy val organic_idfv: String = getString("organic_idfv")
    lazy val advertising_id: String = getString("advertising_id")
    lazy val advertising_id_sp: String = getString("advertising_id_sp")

    lazy val uri: String = getString("uri")
    lazy val referrer: String = getString("referrer")
    lazy val landing_page: String = getString("landing_page")
    lazy val test_info: String = getString("test_info")
    lazy val media_source: String = getString("media_source")
}
