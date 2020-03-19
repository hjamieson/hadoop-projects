package org.oclc.hbase.perf.spark

import org.junit.runner.RunWith
import org.oclc.hbase.perf.spark.util.JsonMapper
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class JsonMapperSpec extends FunSuite with Matchers {


  test("we can marshall an object to json") {
    val hugh = Dummy("hugh", "jamieson", 61)
    val str = JsonMapper.toJson(hugh)
    assert(str contains ("jamieson"))
    println(str)
  }

  test("we can round-trip an object") {
    val alex = Dummy("alex", "jamieson", 16)
    val str = JsonMapper.toJson(alex)
    assert(str contains ("jamieson"))
    val result = JsonMapper.fromJson[Dummy](str)
    assert(result.isInstanceOf[Dummy])
    assert(result.age == 16)

  }

  test("we can output a standard MAP"){
    val map = scala.collection.mutable.Map[String,String]()
    map("field1") = "value1"
    map("field2") = "value2"
    map("field3") = "value3"
    val json = JsonMapper.toJson(map)
    println(json)
    assert(json.contains("field2"))
    assert(json.contains("value2"))

  }
}
