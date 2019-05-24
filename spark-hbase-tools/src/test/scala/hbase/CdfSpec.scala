package hbase

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.xml.{NodeSeq, XML}

class CdfSpec extends FlatSpec with Matchers {
  val source = Source.fromURL(getClass.getResource("/sample1.cdf"))
  val sample1 = source.getLines().mkString
  source.close()
  val src2 = Source.fromURL(getClass.getResource("/sample2.cdf"))
  val sample2 = src2.getLines().mkString

  "A CDF file" should "be loaded for a test" in {
    assert(sample1.length > 0)
    assert(sample1.contains("CDFRec"))
    succeed
  }

  "sample1" should "have a v245 field" in {
    val temp = XML.loadString(sample1)
    val v245 = temp \ "v245"
    assert(v245.isInstanceOf[NodeSeq])
    println(v245)
    assert((v245 \ "sa" \ "d").text == "The Rand McNally book of favorite pastimes /")
  }

  "sample2" should "have a v100 field" in {
    val temp = XML.loadString(sample2)
    val v100 = temp \ "v100"
    assert(v100.isInstanceOf[NodeSeq])
    println(v100)
    assert((v100 \ "sa" \ "d").text == "Irmen, Kenneth E.")
  }


}
