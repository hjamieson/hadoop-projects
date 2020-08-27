package org.oclc.hbase.snoop2.impl

import org.oclc.hbase.snoop2.impl.JsonUtil.toJson
import org.scalatest.flatspec.AnyFlatSpec

class JsonSpec extends AnyFlatSpec {
  case class Person(name: String, age:Int)

  "toJson" should "convert object to JSON"in {
    val p = Person("hugh", 62)
    val json = toJson(p)
    println(json)
    assert(json.contains("hugh"))
  }

}
