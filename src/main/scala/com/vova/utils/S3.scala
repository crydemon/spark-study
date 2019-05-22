package com.vova.utils

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._

object S3Config {
  lazy val keyId: String = System.getenv("AWS_ACCESS_KEY_ID")
  lazy val accessKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")


  private lazy val buildEnv: String = System.getenv("BUILD_ENV") match {
    case "local" => "local.conf"
    case _ => "prod.conf"
  }

  private val conf: Config = ConfigFactory.systemEnvironment()
    .withFallback(ConfigFactory.load(buildEnv))

  def getString(path: String): String = {
    conf.getString(path)
  }
}

object S3 {

  def loadEnrichData(spark: SparkSession, start: LocalDateTime, end: LocalDateTime): Dataset[Event] = {
    //        import spark.implicits._
    val storagePrefix = S3Config.getString("s3.evt.enrich.root")
    val inFmt = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    val gap = start.until(end, ChronoUnit.HOURS)
    val sources = (0L until gap).map(start.plusHours).map(storagePrefix + _.format(inFmt) + "/*")
    implicit val eventEncoder: Encoder[Event] = org.apache.spark.sql.Encoders.kryo[Event]
    //spark.read.textFile("d://enrich.gz")
    spark.read.textFile(sources: _*)
      .filter(!_.contains("Googlebot"))
      .flatMap(line => Event.parse(line).toOption.flatMap { e =>
        if (e.app_id.isDefined && e.app_id.forall(_.startsWith("vova")) &&
          e.br_type.forall(!_.contains("Robot"))) {
          Some(e)
        } else {
          None
        }
      })
  }

  def dedup(ds: Dataset[Event]): Dataset[Event] = {
    implicit val eventEncoder: Encoder[Event] = org.apache.spark.sql.Encoders.kryo[Event]
    implicit val tupleEncoder: Encoder[(Event, String)] = Encoders.tuple[Event, String](eventEncoder, Encoders.STRING)
    ds.map(e => (e, e.event_fingerprint.orNull))
      .toDF("value", "event_fingerprint")
      .dropDuplicates("event_fingerprint")
      .drop("event_fingerprint")
      .as[Event]
  }

}


