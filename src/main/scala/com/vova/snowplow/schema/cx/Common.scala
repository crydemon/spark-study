package com.vova.snowplow.schema.cx

import com.vova.snowplow.schema.BaseWrappedJson
import io.circe.JsonObject

class Common(val json:JsonObject) extends BaseWrappedJson {
    lazy val country: String = getString("country")
    lazy val language: String = getString("language")
    lazy val currency: String = getString("currency")
    lazy val gender: String = getString("gender")

    lazy val user_unique_id: Long = getLong("user_unique_id", -1)

    lazy val user_id: String = getString("user_id")

    lazy val domain_user_id: String = getString("domain_user_id")

    lazy val page_code: String = getString("page_code")

    lazy val uri: String = getString("uri")

    lazy val account_class: String = getString("account_class")
}
