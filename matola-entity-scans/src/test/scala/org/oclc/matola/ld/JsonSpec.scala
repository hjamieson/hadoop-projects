package org.oclc.matola.ld

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.scalatest.FlatSpec

class JsonSpec extends FlatSpec {

  "An mapper"  should "convert DataRow to JSON" in {
    val sample = RowData("123",1,2,3)
      val om = new ObjectMapper() with ScalaObjectMapper
    om.registerModule(DefaultScalaModule)
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val json = om.writeValueAsString(sample)
    assert(json.contains("123"))
    println(json)
  }

  "The JsonUtil" should "serialize RowData to Json" in {
    val json = JsonUtil.toJson(RowData("hugh",10,11,12))
    assert(json.contains("hugh"))
  }

}
