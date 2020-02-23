package org.oclc.hbase.devtools

import org.oclc.hbase.spark.job.serializer.JsonMapper
import org.scalatest.{FunSuite, Matchers}

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
}
