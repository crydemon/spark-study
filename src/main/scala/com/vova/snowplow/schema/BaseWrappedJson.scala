package com.vova.snowplow.schema

import io.circe.{Json, JsonObject}
import org.apache.spark.sql.Row

import scala.util.Try

abstract class BaseWrappedJson {
  val json: JsonObject


  def getArray(name: String): Vector[Json] = {
    if (json != null) {
      val field = json(name).orNull
      if (field != null) {
        return field.asArray.orNull
      }
    }
    null
  }

  def getString(name: String): String = {
    if (json != null) {
      val field = json(name).orNull
      if (field != null) {
        if (field.isString) {
          return field.asString.orNull
        } else if (field.isNumber) {
          return field.asNumber.toString
        }
      }
    }
    null
  }

  def getLong(name: String, default: Long = 0): Long = {
    if (json != null) {
      val field = json(name).orNull
      if (field != null) {
        if (field.isNumber) {
          return field.asNumber.flatMap(x => x.toLong).getOrElse(default)
        } else if (field.isString) {
          return field.asString.flatMap(x => Try(x.toLong).toOption).getOrElse(default)
        }
      }
    }
    default
  }
}
