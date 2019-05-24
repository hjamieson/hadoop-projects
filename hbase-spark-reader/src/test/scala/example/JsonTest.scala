package example

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.{FlatSpec, FunSpec, Matchers}
import collection.JavaConverters._

class JsonTest extends FlatSpec with Matchers {
  "A json string" should "parse by OM" in {
    val testJson = "{ \"name\": \"hugh\",\"sex\":\"male\",\"age\":\"100\" }"
    val om = new ObjectMapper()
    val obj = om.readValue(testJson, classOf[java.util.Map[String, String]]).asScala
    assert(obj("name") == "hugh")
    assert(obj("age") == "100")

  }

  "A numeric value" should "be converted to a numeric type" in {
    val testJson = "{ \"name\": \"hugh\",\"sex\":\"male\",\"age\":100, \"temp\": 98.6 }"
    val om = new ObjectMapper()
    val obj = om.readValue(testJson, classOf[java.util.Map[String, Any]]).asScala
    assert(obj("name") == "hugh")
    assert(obj("age") == 100)
    assert(obj("age").isInstanceOf[Integer])
    assert(obj("temp").isInstanceOf[Double])
  }

  "Use match" should "discover types" in {
    val testJson = "{ \"name\": \"hugh\",\"sex\":\"male\",\"age\":100, \"temp\": 98.6 }"
    val om = new ObjectMapper()
    val obj = om.readValue(testJson, classOf[java.util.Map[String, Any]]).asScala

//    obj.seq.map({
//      case s : String => s
//    })
  }
}
