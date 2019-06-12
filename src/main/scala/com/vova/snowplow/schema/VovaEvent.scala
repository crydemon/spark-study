package com.vova.snowplow.schema

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

class VovaEvent(override val event: Event)  extends VovaEventHelper(event) {


}
