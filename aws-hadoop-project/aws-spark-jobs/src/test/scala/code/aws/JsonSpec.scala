package code.aws

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, OptionModule, TupleModule}
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class JsonSpec extends FlatSpec {
  val cdf2 = Source.fromURL(getClass.getResource("/sample2.cdf")).getLines().mkString

  "A marshaller" should "take a case class and return a string" in {
    val book = Book(10, "How To Run", "I. M. Fast")
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    val json = om.writeValueAsString(book)
    println(json)
    assert(json.contains("author"))
  }

  "ObjectMapper" should "convert a bib to json" in {
    val bib = BibHelper("111", cdf2)
    val om = new ObjectMapper()
    val f = om.getFactory
    val writer = new StringWriter()
    val g = f.createGenerator(writer)
    g.writeStartObject()
    g.writeStringField("id", bib.key)
    g.writeStringField("title", bib.title.get)
    g.writeStringField("author", bib.author.get)
    g.writeEndObject()
    g.close()
    println(writer.toString)
    assert(writer.toString.contains("author"))
  }

  "BibHelper" should "provide a JSON value given a mapper" in {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    val bib = BibHelper("111", cdf2)
    val json = bib.toJson(om)
    assert(json.contains("title"))
    assert(json.contains("author"))
  }

  "BibHelper" should "provide a JSON value" in {
    val bib = BibHelper("111", cdf2)
    val json = bib.toJson
    assert(json.contains("title"))
    assert(json.contains("author"))
  }
}


case class Book(id: Int, title: String, author: String)