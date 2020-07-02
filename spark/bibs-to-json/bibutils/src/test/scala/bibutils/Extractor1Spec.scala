package bibutils

import org.scalatest.FunSuite

import scala.xml.XML

class Extractor1Spec extends FunSuite {
  val cdf = XML.loadFile("bibutils/src/test/resources/bib1.xml")

  test("we can extract title"){
    val pojo = Extractor1(cdf)
    assert(pojo.title.contains("Rand"))
    assert(pojo.author == "N/A")
  }

  test("we can get the 100 record"){
    val cdf = XML.loadFile("bibutils/src/test/resources/bib2.xml")
    val davinci = Extractor1(cdf)
    assert(davinci.title.contains("The Da Vinci code"))
    assert(davinci.author == "Brown, Dan,")
  }

  test("we can extract country"){
    val cdf = XML.loadFile("bibutils/src/test/resources/bib2.xml")

    val pojo = Extractor1(cdf)
    assert(pojo.country == "na")
    println(pojo)

  }

}
