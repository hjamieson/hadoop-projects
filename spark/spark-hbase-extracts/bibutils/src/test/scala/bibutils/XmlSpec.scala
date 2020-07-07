package bibutils

import java.io.File

import org.scalatest.FunSuite

import scala.xml.XML

class XmlSpec extends FunSuite {
  val doc = <book>
    <title>"Fun With Scala"</title> <author>Huey</author>
  </book>
  test("we can embed XML in scala") {
    assert((doc \ "title")(0).label == "title" )
    assert((doc \ "author").length == 1)
    assert((doc \ "author")(0).text == "Huey")
  }

  test("we can iterate thru a result"){
    val doc = <a>
    <b>
      <c>dog</c>
    </b>
      <d>
      <c>cat</c>
      <c>mouse</c>
      </d>
    </a>
    val cs = doc \\ "c"
    assert(cs.length == 3)
    println(cs.text)
    println(cs.mkString(":"))
  }

  test("we can load xml from file"){
    val doc = XML.loadFile(new File("bibutils/src/test/resources/bib1.xml"))
    assert(doc.length > 0)
    val v245 = doc \ "v245"
    assert(v245.length > 0)
    println(v245.text)
  }

  test("we don't crash on missing"){
    val doc = <book><author>Huey</author><pages>122</pages></book>
    val notFound = doc \ "page"
    assert(notFound.isEmpty)
    assert(notFound.headOption.isEmpty)
  }
}
