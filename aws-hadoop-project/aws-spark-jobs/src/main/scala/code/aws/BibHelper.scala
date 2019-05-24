package code.aws

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.xml.XML

case class BibHelper(key: String, cdf: String) {
  val root = XML.loadString(cdf)

  def title: Option[String] = {
    val seq = root \ "v245"
    if (seq.isEmpty) None
    else Some(seq.text)
  }

  def author: Option[String] = {
    val seq = root \ "v100"
    if (seq.isEmpty) None
    else Some(seq.text)
  }

  def publicationDate: Option[String] = {
    val seq = root \ "v260" \ "sc" \ "d"
    if (seq.nonEmpty) Some(seq.text)
    else None
  }

  def documentSize: Int = cdf.length

  def stdrt: Option[String] = {
    val seq = root \ "Admin" \ "OCLCDef"\ "StdRT"
    if (seq.nonEmpty) Some(seq.text)
    else None
  }

  def toJson(om: ObjectMapper): String = {
    val factory = om.getFactory
    val writer = new StringWriter()
    val g = factory.createGenerator(writer)
    g.writeStartObject()
    g.writeStringField("id",key)
    g.writeStringField("title",title.getOrElse("undefined"))
    g.writeStringField("author",author.getOrElse("undefined"))
    g.writeStringField("publicationDate",publicationDate.getOrElse("undefined"))
    g.writeStringField("stdrt",stdrt.getOrElse("undefined"))
    g.writeEndObject()
    g.close()
    writer.toString
  }

  def toJson: String = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    toJson(om)
  }
}

object JsonMapper {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def get: ObjectMapper = {
    mapper
  }
}