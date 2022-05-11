package com.jsonparser.spark.connector

import play.api.libs.json.{Format, Json}

/**
 * Model for external configuration file
 * @param key
 * @param value
 */
case class KeyValuePair(var key:String, var value:String) {
  override def equals(that: Any): Boolean = true
}
object KeyValuePair {
  implicit val jsonFormat: Format[KeyValuePair] = Json.using[Json.WithDefaultValues].format[KeyValuePair]
}
