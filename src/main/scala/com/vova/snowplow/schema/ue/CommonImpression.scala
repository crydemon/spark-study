package com.vova.snowplow.schema.ue

import com.vova.snowplow.schema.{BaseWrappedJson, BaseWrappedRow}
import io.circe.JsonObject
import org.apache.spark.sql.Row

class CommonImpression(val json:JsonObject) extends BaseWrappedJson {
    val list_uri: String = getString("list_uri")
    val list_name: String = getString("list_name")
    val item_list: Seq[_CommonImpressionUnit] = {
        val seq = getArray("item_list")
        if (seq != null) seq.map(x => new _CommonImpressionUnit(x.asObject.orNull)) else Seq()
        null
    }
}

class _CommonImpressionUnit(val json:JsonObject) extends BaseWrappedJson {
    val name: String = getString("name")
}
