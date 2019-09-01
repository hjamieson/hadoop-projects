package org.oclc.hbase.devtools.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonMapper {
  val om = new ObjectMapper() with ScalaObjectMapper
  om.registerModule(DefaultScalaModule)
  om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Any): String = om.writeValueAsString(value)

  def fromJson[T: Manifest](json: String): T = om.readValue[T](json)

}
