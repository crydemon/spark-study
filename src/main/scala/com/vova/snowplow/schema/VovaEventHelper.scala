package com.vova.snowplow.schema

import java.time.Instant

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.UnstructEvent
import com.vova.snowplow.schema.cx.{AppCommon, ClientSession, Common, Mobile}
import com.vova.snowplow.schema.ue._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row

import scala.util.Try

object VovaEventHelper{
    val VENDOR_ARTEMIS = "com.artemis"
    val VENDOR_SNOWPLOW = "com.snowplowanalytics.snowplow"
    val VENDOR_GOOGLE = "com.google.analytics"
}

class VovaEventHelper(val event: Event) {
    import VovaEventHelper._

    def getContext(vendor: String, name: String) :JsonObject  = {
        event.contexts.data.find(data => {
            data.schema.name == name && data.schema.vendor == vendor
        }).flatMap(_.data.asObject).orNull
    }

    def getUnstructEvent(vendor: String, name: String) :JsonObject = {
        val data = event.unstruct_event.data.orNull
        if (data != null && data.schema.name == name && data.schema.vendor == vendor) {
            data.data.asObject.orNull
        } else {
            null
        }
    }

    def getRow(name: String) :Row = null

    lazy val app_common_context: AppCommon = new AppCommon(getContext(VENDOR_ARTEMIS, "app_common"))
    lazy val common_context: Common = new Common(getContext(VENDOR_ARTEMIS, "common"))
    lazy val mobile_context: Mobile = new Mobile(getContext(VENDOR_SNOWPLOW, "mobile_context"))
    lazy val session_context: ClientSession = new ClientSession(getContext(VENDOR_SNOWPLOW, "client_session"))

    lazy val app_id: String = event.app_id.orNull
    lazy val name_tracker: String = event.name_tracker.orNull

    lazy val created_ts: String = event.dvce_created_tstamp.map(_.toString).orNull
    lazy val collector_ts: String = event.collector_tstamp.toString
    lazy val derived_ts: String = event.derived_tstamp.map(_.toString).orNull

    lazy val platform: String = event.platform.orNull

    lazy val os_type: String = Option(platform match {
        case "mob" => mobile_context.os_type
        case _ => event.os_family.orNull
    }).map(_.toLowerCase).orNull

    lazy val raw_event_name: String = event.event_name.getOrElse("")
    lazy val event_name: String = raw_event_name.split("-").head
    lazy val event_groups: String = raw_event_name.split("-").tail.mkString(",")

    lazy val ecommerce_action_event: EcommerceAction = new EcommerceAction(getUnstructEvent(VENDOR_GOOGLE, "enhanced_ecommerce_action"))
    lazy val goods_impression_event: GoodsImpression = new GoodsImpression(getUnstructEvent(VENDOR_ARTEMIS, "goods_impression"))
    lazy val goods_click_event: GoodsClick = new GoodsClick(getUnstructEvent(VENDOR_ARTEMIS, "goods_click-link_click"))
    lazy val common_impression_event: CommonImpression = new CommonImpression(getUnstructEvent(VENDOR_ARTEMIS, "common_impression"))
    lazy val common_click_event: CommonClick = new CommonClick(getUnstructEvent(VENDOR_ARTEMIS, "common_click-link_click"))
    lazy val order_process_event: OrderProcess = new OrderProcess(getUnstructEvent(VENDOR_ARTEMIS, "order_process"))
    lazy val test_create_event: TestCreate = new TestCreate(getUnstructEvent(VENDOR_ARTEMIS, "test_create"))

    lazy val page_url: String = platform match {
        case "mob" => app_common_context.uri
        case _ => event.page_url.orNull
    }

    lazy val page_referrer: String = platform match {
        case "mob" => app_common_context.referrer
        case _ => event.page_referrer.orNull
    }

    private lazy val web_goods_pattern = "-m(\\d+)".r
    private lazy val mob_goods_pattern = "goods_id=(\\d+)".r
    lazy val page_goods_id: Long = Try(platform match {
        case "mob" => Option(page_url).flatMap(mob_goods_pattern.findFirstMatchIn(_)).map(_.group(1)).getOrElse("-1").toLong
        case "web" => Option(page_url).flatMap(web_goods_pattern.findFirstMatchIn(_)).map(_.group(1)).getOrElse("-1").toLong
        case _ => -1
    }).getOrElse(-2)

    private lazy val web_route_pattern = "-r(\\d+)".r
    private lazy val mob_route_pattern = "route_sn=(\\d+)".r
    lazy val page_route_sn: Long = Try(platform match {
        case "mob" => Option(page_url).flatMap(mob_route_pattern.findFirstMatchIn(_)).map(_.group(1)).getOrElse("-1").toLong
        case "web" => Option(page_url).flatMap(web_route_pattern.findFirstMatchIn(_)).map(_.group(1)).getOrElse("-1").toLong
        case _ => -1
    }).getOrElse(-2)

    lazy val session_id: String = platform match {
        case "mob" => session_context.session_id
        case _ => event.domain_sessionid.orNull
    }

    lazy val domain_userid: String = Option(common_context.domain_user_id)
        .map(_.trim).filter(_.nonEmpty)
        .orElse(event.domain_userid)
        .orNull

    lazy val user_id: String = Option(common_context.user_id)
        .map(_.trim).filter(_.nonEmpty)
        .orElse(event.user_id)
        .orNull
}
